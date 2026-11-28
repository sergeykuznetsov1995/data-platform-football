from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from scrapers.transfermarkt.attendance import (
    ATTENDANCE_COLUMNS,
    ATTENDANCE_KEY,
    ATTENDANCE_STATUSES,
    AttendancePage,
    AttendanceParseError,
    AttendanceExtraction,
    AttendanceResult,
    build_attendance_dq,
    canonical_timestamp,
    confirmed_empty_attendance,
    error_attendance,
    extract_attendance,
    not_applicable_attendance,
    parse_attendance,
    unavailable_attendance,
    unknown_attendance,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).parents[2] / 'fixtures' / 'transfermarkt' / 'attendance'
START = 'https://www.transfermarkt.com/premier-league/besucherzahlen/wettbewerb/GB1/saison_id/2025'
PAGE_2 = START + '?page=2'
CAPTURED = '2026-11-20T12:00:00+00:00'
EXTRACTED = '2026-11-20T12:01:00+00:00'


def make_page(body: bytes, url: str = START, *, capture_id: str = 'capture-1',
              captured_at: str = CAPTURED, status_code: int = 200) -> AttendancePage:
    return AttendancePage(
        url=url,
        body=body,
        capture_id=capture_id,
        body_hash=hashlib.sha256(body).hexdigest(),
        captured_at=captured_at,
        status_code=status_code,
    )


def fixture_page(name: str, url: str = START, **kwargs) -> AttendancePage:
    return make_page((FIXTURES / name).read_bytes(), url, capture_id=f'capture-{name}', **kwargs)


def single_page() -> AttendancePage:
    return fixture_page('single_page.html')


def crest_page() -> AttendancePage:
    return fixture_page('with_league_table.html')


LEAGUE_BOX = b'<div class="box"><h2 class="content-box-headline">Table</h2>'
EMPTY_ATTENDANCE_GRID = (b'<div class="box"><div class="grid-view"><table class="items">'
                         b'<thead><tr><th>#</th><th>Stadium</th><th>Capacity</th>'
                         b'<th>Spectators</th><th>Average</th></tr></thead><tbody></tbody></table></div></div>')


def multi_pages() -> tuple[AttendancePage, AttendancePage]:
    return (
        fixture_page('page_1.html'),
        fixture_page('page_2.html', PAGE_2, captured_at=CAPTURED),
    )


def without_first_club(body: bytes) -> bytes:
    row = body.split(b'<tr class="odd">', 1)[1].split(b'<tr class="even">', 1)[0].rsplit(b'</tr>', 1)[0]
    return body.replace(b'<tr class="odd">' + row + b'</tr>', b'')


def extract_single(**kwargs) -> AttendanceExtraction:
    return extract_attendance(
        [single_page()],
        start_url=START,
        competition_id='GB1',
        edition_id='2025',
        scope_id='scope-attendance',
        run_id='run-attendance',
        universe_version='registry-rev-1',
        extracted_at=EXTRACTED,
        **kwargs,
    )


def test_single_page_preserves_published_club_rows_and_separate_values():
    result = parse_attendance([single_page()], start_url=START, competition_id='GB1', edition_id=2025)
    assert result.status == 'observed_present'
    assert result.page_count == 1
    assert len(result.rows) == 2
    arsenal, chelsea = result.rows
    assert arsenal.natural_key == ('GB1', '2025', '11', CAPTURED)
    assert arsenal.club_name == 'Arsenal FC'
    assert arsenal.stadium_name == 'Emirates Stadium'
    assert arsenal.stadium_url == 'https://www.transfermarkt.com/arsenal-fc/stadion/verein/11'
    assert arsenal.capacity == 60704
    assert arsenal.spectators_total == 1151376
    assert arsenal.spectators_average == 60598
    assert arsenal.matches == 19
    assert arsenal.sold_out == 17
    assert arsenal.capacity_filled_raw == '99,8 %'
    assert arsenal.capacity_raw == '60.704'
    assert arsenal.spectators_total_raw == '1.151.376'
    assert arsenal.spectators_average_raw == '60.598'
    assert chelsea.club_id == '631'
    assert chelsea.spectators_total == 760000
    assert chelsea.spectators_average == 40000
    assert chelsea.sold_out == 0
    assert {row.club_name for row in result.rows} == {'Arsenal FC', 'Chelsea FC'}


def test_extraction_has_full_lineage_deterministic_batch_and_no_match_fields():
    extraction = extract_single(final_season=True, final_season_proof='registry:GB1:2025:closed')
    rows = extraction.rows
    assert len(rows) == 2
    assert extraction.batch_id.startswith('tm-attendance-batch-')
    for row in rows:
        assert tuple(row) == ATTENDANCE_COLUMNS
        assert row['final_season'] is True
        assert row['final_season_proof'] == 'registry:GB1:2025:closed'
        assert row['observed_at'] == row['captured_at'] == CAPTURED
        assert row['extracted_at'] == row['_ingested_at'] == EXTRACTED
        assert row['source_capture_id'] == 'capture-single_page.html'
        assert row['source_body_hash'] == hashlib.sha256(single_page().body).hexdigest()
        assert row['parser_revision'] == 'attendance-v2'
        assert row['schema_revision'] == '1'
        assert row['scope_id'] == 'scope-attendance'
        assert row['run_id'] == 'run-attendance'
        assert row['universe_version'] == 'registry-rev-1'
        assert row['_batch_id'] == extraction.batch_id
        assert 'match_id' not in row and 'match_date' not in row
    assert json.loads(json.dumps(rows)) == list(rows)
    assert extraction.canonical_sha256() == extract_single(
        final_season=True, final_season_proof='registry:GB1:2025:closed',
    ).canonical_sha256()


def test_structural_dq_reconciles_expected_participants():
    report = build_attendance_dq(extract_single(), expected_club_ids=[11, 631])
    assert report['passed'] is True
    assert report['expected_clubs'] == report['observed_clubs'] == 2
    assert report['row_count'] == 2
    assert report['missing_club_ids'] == report['unexpected_club_ids'] == []
    assert report['status_counts'] == {'observed_present': 2}
    assert report['errors'] == []


def test_multi_page_unions_clubs_and_preserves_each_page_lineage():
    result = parse_attendance(multi_pages(), start_url=START, competition_id='GB1', edition_id=2025)
    assert result.page_count == 2
    assert {row.club_id for row in result.rows} == {'11', '631'}
    by_club = {row.club_id: row for row in result.rows}
    assert by_club['11'].source_url == START
    assert by_club['11'].source_capture_id == 'capture-page_1.html'
    assert by_club['631'].source_url == PAGE_2
    assert by_club['631'].source_capture_id == 'capture-page_2.html'


def test_multi_page_snapshot_separates_observation_from_page_capture_times():
    first, second = multi_pages()
    later = '2026-11-20T12:00:05+00:00'
    second = replace(second, captured_at=later)
    result = parse_attendance(
        [first, second], start_url=START, competition_id='GB1', edition_id=2025,
        observed_at=first.captured_at,
    )
    assert {row.observed_at for row in result.rows} == {CAPTURED}
    assert {row.captured_at for row in result.rows} == {CAPTURED, later}
    assert {row.source_capture_id for row in result.rows} == {
        'capture-page_1.html', 'capture-page_2.html',
    }


def test_multi_page_requires_saved_next_page():
    with pytest.raises(AttendanceParseError, match='pagination_page_missing'):
        parse_attendance([multi_pages()[0]], start_url=START, competition_id='GB1', edition_id=2025)


def test_multi_page_rejects_cycle():
    first, second = multi_pages()
    body = second.body.replace(
        b'</body>',
        f'<a rel="next" href="{START}">again</a></body>'.encode(),
    )
    cyclic = make_page(body, PAGE_2, capture_id='capture-cyclic')
    with pytest.raises(AttendanceParseError, match='pagination_cycle'):
        parse_attendance([first, cyclic], start_url=START, competition_id='GB1', edition_id=2025)


@pytest.mark.parametrize('href', [
    'https://example.com/premier-league/besucherzahlen/wettbewerb/GB1/saison_id/2025?page=2',
    '/premier-league/besucherzahlen/wettbewerb/GB1/saison_id/2024?page=2',
    '/premier-league/besucherzahlen/wettbewerb/ES1/saison_id/2025?page=2',
    '/premier-league/besucherzahlen/wettbewerb/GB1/saison_id/2025?page=2&sort=club',
])
def test_multi_page_rejects_changed_identity(href):
    body = single_page().body.replace(
        b'</body>', f'<a rel="next" href="{href}">next</a></body>'.encode(),
    )
    with pytest.raises(AttendanceParseError, match='pagination_identity_invalid'):
        parse_attendance([make_page(body)], start_url=START, competition_id='GB1', edition_id=2025)


def test_multi_page_rejects_ambiguous_next_links():
    body = single_page().body.replace(
        b'</body>',
        f'<a rel="next" href="{PAGE_2}">two</a><a rel="next" href="{START}?page=3">three</a></body>'.encode(),
    )
    with pytest.raises(AttendanceParseError, match='pagination_next_ambiguous'):
        parse_attendance([make_page(body)], start_url=START, competition_id='GB1', edition_id=2025)


def test_page_hash_and_capture_timestamp_are_verified():
    body = single_page().body
    with pytest.raises(AttendanceParseError, match='body_hash_mismatch'):
        AttendancePage(START, body, 'capture', '0' * 64, CAPTURED)
    with pytest.raises(AttendanceParseError, match='timestamp_invalid'):
        make_page(body, captured_at='2026-11-20 12:00:00')
    assert canonical_timestamp(CAPTURED) == CAPTURED


def test_non_200_is_unavailable_and_never_empty():
    page = fixture_page('single_page.html', status_code=503)
    result = parse_attendance([page], start_url=START, competition_id='GB1', edition_id=2025)
    assert result.status == 'unavailable'
    assert result.status_reason == 'http_503'
    extraction = AttendanceExtraction(
        result, 'scope', 'run', 'universe', EXTRACTED,
    )
    assert build_attendance_dq(extraction, [11, 631])['errors'] == ['unresolved_capture']


def test_later_unavailable_page_discards_partial_rows():
    first, _ = multi_pages()
    unavailable = fixture_page('page_2.html', PAGE_2, status_code=404)
    result = parse_attendance([first, unavailable], start_url=START, competition_id='GB1', edition_id=2025)
    assert result.status == 'unavailable'
    assert result.rows == ()


def test_malformed_capture_raises_and_extraction_marks_error():
    broken = make_page(b'<html>blocked</html>')
    with pytest.raises(AttendanceParseError, match='attendance_table_missing'):
        parse_attendance([broken], start_url=START, competition_id='GB1', edition_id=2025)
    extraction = extract_attendance(
        [broken], start_url=START, competition_id='GB1', edition_id=2025,
        scope_id='scope', run_id='run', universe_version='universe', extracted_at=EXTRACTED,
    )
    assert extraction.result.status == 'error'
    assert extraction.result.status_reason == 'attendance_table_missing'
    assert build_attendance_dq(extraction, [])['errors'] == ['unresolved_capture']


def test_empty_layout_is_unknown_without_explicit_proof():
    result = parse_attendance(
        [fixture_page('empty.html')], start_url=START, competition_id='GB1', edition_id=2025,
    )
    assert result.status == 'unknown'
    assert result.status_reason == 'empty_layout_unproven'
    empty = confirmed_empty_attendance(completeness_proof='capture:empty-layout-v1')
    extraction = AttendanceExtraction(empty, 'scope', 'run', 'universe', EXTRACTED)
    assert build_attendance_dq(extraction, [])['passed'] is True
    with pytest.raises(AttendanceParseError, match='proof_missing'):
        confirmed_empty_attendance(completeness_proof='')


def test_not_applicable_requires_proof():
    result = not_applicable_attendance(applicability_proof='registry:no-attendance-context')
    assert result.status == 'not_applicable'
    extraction = AttendanceExtraction(result, 'scope', 'run', 'universe', EXTRACTED)
    assert build_attendance_dq(extraction, [11])['passed'] is True
    with pytest.raises(AttendanceParseError, match='proof_missing'):
        not_applicable_attendance(applicability_proof='')


def test_explicit_none_and_published_zero_remain_distinct():
    result = parse_attendance(
        [fixture_page('explicit_none.html')], start_url=START, competition_id='GB1', edition_id=2025,
    )
    arsenal, chelsea = result.rows
    assert arsenal.observation_status == 'observed_present'
    assert arsenal.spectators_total == 0
    assert arsenal.spectators_total_raw == '0'
    assert arsenal.capacity is None and arsenal.spectators_average is None
    assert chelsea.observation_status == 'explicit_none'
    assert chelsea.capacity is None
    assert chelsea.spectators_total is None
    assert chelsea.spectators_average is None
    assert chelsea.stadium_name is None


def test_all_explicit_none_rows_determine_capture_status():
    body = without_first_club(fixture_page('explicit_none.html').body)
    result = parse_attendance(
        [make_page(body)], start_url=START, competition_id='GB1', edition_id=2025,
    )
    assert result.status == 'explicit_none'
    assert len(result.rows) == 1
    extraction = AttendanceExtraction(result, 'scope', 'run', 'universe', EXTRACTED)
    report = build_attendance_dq(extraction, [631])
    assert report['passed'] is True
    assert report['status_counts'] == {'explicit_none': 1}


def test_capacity_without_attendance_is_explicit_none():
    body = fixture_page('explicit_none.html').body.replace(
        b'<td>-</td><td>-</td><td>-</td>',
        b'<td>40,173</td><td>-</td><td>-</td>',
    )
    result = parse_attendance(
        [make_page(body)], start_url=START, competition_id='GB1', edition_id=2025,
    )
    chelsea = result.rows[1]
    assert chelsea.capacity == 40173
    assert chelsea.spectators_total is None and chelsea.spectators_average is None
    assert chelsea.observation_status == 'explicit_none'


@pytest.mark.parametrize('raw', [b'<td></td>', b'<td>?</td>'])
def test_blank_or_unknown_attendance_is_not_explicit_none(raw):
    body = fixture_page('explicit_none.html').body.replace(
        b'<td>-</td><td>-</td><td>-</td><td></td>',
        b'<td>-</td>' + raw + b'<td>-</td><td></td>',
    )
    result = parse_attendance(
        [make_page(body)], start_url=START, competition_id='GB1', edition_id=2025,
    )
    chelsea = result.rows[1]
    assert chelsea.observation_status == 'unknown'
    extraction = AttendanceExtraction(result, 'scope', 'run', 'universe', EXTRACTED)
    assert 'row_status_unresolved' in build_attendance_dq(extraction, [11, 631])['errors']


def test_all_unknown_rows_determine_unknown_capture_status():
    body = fixture_page('explicit_none.html').body.replace(
        b'<td>-</td><td>-</td><td>-</td><td></td>',
        b'<td>-</td><td></td><td>-</td><td></td>',
    )
    body = without_first_club(body)
    result = parse_attendance(
        [make_page(body)], start_url=START, competition_id='GB1', edition_id=2025,
    )
    assert result.status == 'unknown'
    assert len(result.rows) == 1
    extraction = AttendanceExtraction(result, 'scope', 'run', 'universe', EXTRACTED)
    errors = build_attendance_dq(extraction, [631])['errors']
    assert errors == ['row_status_unresolved', 'unresolved_capture']


def test_nbsp_and_spaces_are_thousands_separators():
    body = single_page().body.replace('60.704'.encode(), '60\u00a0704'.encode())
    body = body.replace(b'1.151.376', b'1 151 376')
    result = parse_attendance(
        [make_page(body)], start_url=START, competition_id='GB1', edition_id=2025,
    )
    assert result.rows[0].capacity == 60704
    assert result.rows[0].spectators_total == 1151376


def test_final_season_marker_requires_explicit_proof():
    with pytest.raises(AttendanceParseError, match='final_season_proof_missing'):
        extract_single(final_season=True)
    with pytest.raises(AttendanceParseError, match='final_season_proof_unexpected'):
        extract_single(final_season_proof='not-final')


def test_dq_reports_missing_and_unexpected_clubs():
    assert build_attendance_dq(extract_single(), [11])['errors'] == ['participants_unexpected']
    assert build_attendance_dq(extract_single(), [11, 631, 999])['errors'] == ['participants_missing']


def test_dq_rejects_same_club_at_two_observation_times_in_one_capture():
    parsed = parse_attendance([single_page()], start_url=START, competition_id='GB1', edition_id=2025)
    first = parsed.rows[0]
    other_time = '2026-11-20T13:00:00+00:00'
    duplicate = replace(first, observed_at=other_time, captured_at=other_time)
    result = AttendanceResult('observed_present', (first, duplicate))
    extraction = AttendanceExtraction(result, 'scope', 'run', 'universe', EXTRACTED)
    report = build_attendance_dq(extraction, [11])
    assert 'duplicate_club_observation' in report['errors']
    assert report['passed'] is False


def test_identical_duplicate_is_reconciled_and_conflict_fails():
    body = single_page().body
    row = body.split(b'<tr class="odd">', 1)[1].split(b'<tr class="even">', 1)[0].rsplit(b'</tr>', 1)[0]
    duplicated = body.replace(b'</tbody>', b'<tr class="odd">' + row + b'</tr></tbody>')
    result = parse_attendance(
        [make_page(duplicated)], start_url=START, competition_id='GB1', edition_id=2025,
    )
    assert len(result.rows) == 2
    assert result.reconciled_duplicates == 1
    changed = b'<tr class="odd">' + row.replace(b'60.704', b'60.705') + b'</tr></tbody>'
    conflicted = body.replace(b'</tbody>', changed)
    with pytest.raises(AttendanceParseError, match='attendance_club_conflict'):
        parse_attendance([make_page(conflicted)], start_url=START, competition_id='GB1', edition_id=2025)


def test_same_club_on_another_page_is_not_silently_deduplicated():
    first, _ = multi_pages()
    second_arsenal = make_page(single_page().body, PAGE_2, capture_id='capture-other')
    with pytest.raises(AttendanceParseError, match='attendance_club_conflict'):
        parse_attendance([first, second_arsenal], start_url=START, competition_id='GB1', edition_id=2025)


def test_bad_club_link_and_header_fail_whole_capture():
    bad_club = single_page().body.replace(b'/arsenal-fc/startseite/verein/11', b'/broken')
    with pytest.raises(AttendanceParseError, match='club_identity_invalid'):
        parse_attendance([make_page(bad_club)], start_url=START, competition_id='GB1', edition_id=2025)
    second_bad_link = single_page().body.replace(
        b'<a href="/arsenal-fc/startseite/verein/11">Arsenal FC</a>',
        b'<a href="/arsenal-fc/startseite/verein/11">Arsenal FC</a>'
        b'<a href="/broken/startseite/verein/abc">Broken</a>',
    )
    with pytest.raises(AttendanceParseError, match='club_identity_invalid'):
        parse_attendance([make_page(second_bad_link)], start_url=START, competition_id='GB1', edition_id=2025)
    bad_header = single_page().body.replace(b'<th>Spectators</th>', b'<th>Attendance</th>')
    with pytest.raises(AttendanceParseError, match='attendance_headers_changed'):
        parse_attendance([make_page(bad_header)], start_url=START, competition_id='GB1', edition_id=2025)


def test_page_with_league_table_reads_only_the_attendance_grid_and_the_crest_layout():
    result = parse_attendance([crest_page()], start_url=START, competition_id='GB1', edition_id=2025)
    assert result.status == 'observed_present' and result.page_count == 1
    assert [(row.club_id, row.club_name, row.stadium_name) for row in result.rows] == [
        ('985', 'Manchester United', 'Old Trafford'), ('379', 'West Ham United', 'London Stadium')]
    united, west_ham = result.rows
    # The crest and the club name carry the same season-schedule link.
    assert united.natural_key == ('GB1', '2025', '985', CAPTURED)
    assert united.stadium_url == 'https://www.transfermarkt.com/985tm/stadion/verein/985'
    assert (united.capacity, united.spectators_total, united.spectators_average) == (74158, 1405516, 73975)
    assert (west_ham.capacity, west_ham.spectators_total, west_ham.spectators_average) == (62500, 1184601, 62347)
    # This grid publishes neither matches, sold out nor capacity filled.
    assert united.matches is None and united.sold_out is None and united.capacity_filled_raw == ''
    assert united.observation_status == 'observed_present' and united.status_reason == 'published_values_present'


def test_league_table_alone_is_not_an_attendance_grid_and_two_attendance_grids_stay_ambiguous():
    _head, league = crest_page().body.split(LEAGUE_BOX, 1)
    # A lone foreign grid is one grid whose headers are not the attendance ones.
    with pytest.raises(AttendanceParseError, match='attendance_headers_changed'):
        parse_attendance([make_page(b'<html><body>' + LEAGUE_BOX + league)], start_url=START,
                         competition_id='GB1', edition_id=2025)
    twice = crest_page().body.replace(b'</body>', EMPTY_ATTENDANCE_GRID + b'</body>')
    with pytest.raises(AttendanceParseError, match='attendance_table_ambiguous'):
        parse_attendance([make_page(twice)], start_url=START, competition_id='GB1', edition_id=2025)
    # A changed attendance header is still reported as such beside the league table.
    changed = crest_page().body.replace(b'>Spectators</th>', b'>Attendance</th>')
    with pytest.raises(AttendanceParseError, match='attendance_headers_changed'):
        parse_attendance([make_page(changed)], start_url=START, competition_id='GB1', edition_id=2025)


@pytest.mark.parametrize('old,new,reason', [
    (b'<a href="/west-ham-united/spielplan/verein/379/saison_id/2025" title="West Ham United">West Ham United</a>',
     b'<a href="/west-ham-united/startseite/verein/379">West Ham United</a>', 'club_identity_invalid'),
    (b'<a href="/west-ham-united/spielplan/verein/379/saison_id/2025" title="West Ham United">West Ham United</a>',
     b'<a href="/west-ham-united/spielplan/verein/380/saison_id/2025">West Ham United</a>', 'club_identity_invalid'),
    (b'<a href="/379tm/stadion/verein/379">London Stadium</a>', b'London Stadium', 'attendance_club_layout_changed'),
    (b'<a href="/west-ham-united/spielplan/verein/379/saison_id/2025" title="West Ham United">West Ham United</a>',
     b'<a href="/west-ham-united/spielplan/verein/0379/saison_id/2025">West Ham United</a>', 'club_identity_invalid'),
])
def test_crest_layout_keeps_one_club_link_and_needs_the_stadium_link(old, new, reason):
    body = crest_page().body.replace(old, new)
    with pytest.raises(AttendanceParseError, match=reason):
        parse_attendance([make_page(body)], start_url=START, competition_id='GB1', edition_id=2025)


def test_german_headers_and_thousands_separators_are_supported():
    body = single_page().body
    body = body.replace(b'<th>Stadium</th>', b'<th>Stadion</th>')
    body = body.replace(b'<th>Capacity</th>', '<th>Kapazität</th>'.encode(), 1)
    body = body.replace(b'<th>Capacity</th>', b'<th>Auslastung</th>', 1)
    body = body.replace(b'<th>Spectators</th>', b'<th>Zuschauer</th>')
    body = body.replace(b'<th>Average</th>', b'<th>Schnitt</th>')
    body = body.replace(b'<th>Matches</th>', b'<th>Spiele</th>')
    body = body.replace(b'<th>Sold out</th>', b'<th>Ausv.</th>')
    result = parse_attendance([make_page(body)], start_url=START, competition_id='GB1', edition_id=2025)
    assert result.rows[0].capacity == 60704
    assert result.rows[0].capacity_filled_raw == '99,8 %'


def test_invalid_number_shape_fails_instead_of_guessing_units():
    body = single_page().body.replace(b'60.704', b'60.70')
    with pytest.raises(AttendanceParseError, match='capacity_invalid'):
        parse_attendance([make_page(body)], start_url=START, competition_id='GB1', edition_id=2025)


@pytest.mark.parametrize('value', [b'1.151,376', b'1,151.376'])
def test_mixed_thousands_separators_are_rejected_as_ambiguous(value):
    body = single_page().body.replace(b'1.151.376', value)
    with pytest.raises(AttendanceParseError, match='spectators_total_invalid'):
        parse_attendance([make_page(body)], start_url=START, competition_id='GB1', edition_id=2025)


def test_capacity_fill_requires_a_percent_or_explicit_missing_marker():
    body = single_page().body.replace(b'99,8 %', b'99,8')
    with pytest.raises(AttendanceParseError, match='capacity_filled_invalid'):
        parse_attendance([make_page(body)], start_url=START, competition_id='GB1', edition_id=2025)


def test_status_constructors_cover_unresolved_states():
    assert unknown_attendance('layout_unproven').status == 'unknown'
    assert unavailable_attendance('capture_unavailable').status == 'unavailable'
    assert error_attendance('invalid_html').status == 'error'
    assert ATTENDANCE_STATUSES == {
        'observed_present', 'observed_empty', 'explicit_none', 'not_applicable',
        'unknown', 'unavailable', 'error',
    }
    assert ATTENDANCE_KEY == ('competition_id', 'edition_id', 'club_id', 'observed_at')
