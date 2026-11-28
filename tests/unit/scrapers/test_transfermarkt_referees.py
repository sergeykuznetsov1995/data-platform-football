from __future__ import annotations

from decimal import Decimal
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from scrapers.transfermarkt.referees import (
    REFEREE_AGGREGATE_COLUMNS,
    REFEREE_AGGREGATE_OBSERVATION_KEY,
    REFEREE_AVERAGE_METRICS,
    REFEREE_COUNT_METRICS,
    REFEREE_METRICS,
    REFEREE_OBSERVATION_STATUSES,
    REFEREE_PARSER_REVISION,
    REFEREE_SCHEMA_REVISION,
    RefereeParseError,
    confirmed_empty_referee_directory,
    confirmed_empty_referee_season_aggregates,
    parse_referee_directory_page,
    parse_referee_profile,
    parse_referee_season_aggregates,
    reconcile_referee_directory_pages,
)

pytestmark = pytest.mark.unit

FIXTURES = Path(__file__).resolve().parents[2] / 'fixtures' / 'transfermarkt' / 'referees'
DIRECTORY_COMPACT = (FIXTURES / 'directory_compact.html').read_text()
DIRECTORY_DETAILED = (FIXTURES / 'directory_detailed.html').read_text()
DIRECTORY_ICONS = (FIXTURES / 'directory_icons.html').read_text()
PROFILE = (FIXTURES / 'profile.html').read_text()
HISTORY_COMPACT = (FIXTURES / 'history_compact.html').read_text()
HISTORY_DETAILED = (FIXTURES / 'history_detailed.html').read_text()


def aggregate(result, referee_id, metric, edition_id='2025'):
    return next(
        row for row in result.aggregates
        if (row.referee_id, row.edition_id, row.metric) == (referee_id, edition_id, metric)
    )


def outer_row(html):
    soup = BeautifulSoup(html, 'html.parser')
    return str(soup.select_one('table.items > tbody > tr'))


def test_profile_preserves_identity_and_optional_fields():
    profile = parse_referee_profile(PROFILE, referee_id='00101')
    assert profile.as_dict() == {
        'referee_id': '101',
        'name': 'Ada Example',
        'nationality': 'Imaginaria',
        'birth_date': '1990-03-10',
    }


def test_profile_optional_absence_does_not_discard_identity():
    profile = parse_referee_profile(
        '<h1 class="data-header__headline-wrapper">Ada <strong>Example</strong></h1>',
        referee_id=101,
    )
    assert profile.referee_id == '101'
    assert profile.nationality is None and profile.birth_date is None


def test_compact_directory_preserves_counts_identity_and_explicit_zero():
    result = parse_referee_directory_page(
        DIRECTORY_COMPACT, competition_id='xy1', edition_id='2025',
    )
    assert len(result.referees) == 2
    assert len(result.aggregates) == 2 * len(REFEREE_COUNT_METRICS)
    entry = next(row for row in result.referees if row.referee_id == '101')
    assert entry.as_dict() == {
        'referee_id': '101', 'name': 'Ada Example',
        'nationality': 'Imaginaria', 'debut_date': '2020-02-01',
    }
    matches = aggregate(result, '101', 'matches_officiated')
    assert matches.value == Decimal('14')
    assert matches.observation_status == 'observed_present'
    assert matches.status_reason == 'numeric'
    assert matches.raw_value == '14'
    assert matches.source_row == 1 and matches.source_column == 'matches_officiated'
    assert matches.as_dict()['value'] == '14'
    assert aggregate(result, '101', 'direct_red_cards').value == Decimal('0')


def test_dash_is_explicit_none_not_zero():
    result = parse_referee_directory_page(
        DIRECTORY_COMPACT, competition_id='XY1', edition_id=2025,
    )
    red = aggregate(result, '202', 'direct_red_cards')
    assert red.value is None
    assert red.observation_status == 'explicit_none'
    assert red.status_reason == 'dash' and red.raw_value == '-'
    assert red.as_dict()['value'] is None


def test_detailed_directory_reads_published_averages():
    result = parse_referee_directory_page(
        DIRECTORY_DETAILED, competition_id='XY1', edition_id='2025',
    )
    assert len(result.aggregates) == len(REFEREE_COUNT_METRICS) + len(REFEREE_AVERAGE_METRICS)
    assert aggregate(result, '101', 'average_yellow_cards').value == Decimal('4.36')
    assert aggregate(result, '101', 'average_direct_red_cards').value == Decimal('0.00')


def test_directory_with_icon_headers_reads_every_metric_and_passes_the_debut_age():
    result = parse_referee_directory_page(
        DIRECTORY_ICONS, competition_id='GB1', edition_id='2025',
    )
    assert len(result.referees) == 2
    assert len(result.aggregates) == 2 * len(REFEREE_METRICS)
    entry = next(row for row in result.referees if row.referee_id == '847')
    assert entry.as_dict() == {
        'referee_id': '847', 'name': 'Anthony Taylor',
        'nationality': 'Cheshire', 'debut_date': '2010-02-03',
    }
    assert aggregate(result, '847', 'matches_officiated').value == Decimal('31')
    assert aggregate(result, '847', 'matches_officiated').source_column == 'matches_officiated'
    assert aggregate(result, '847', 'yellow_cards').value == Decimal('121')
    assert aggregate(result, '847', 'average_yellow_cards').value == Decimal('3.90')
    second_yellow = aggregate(result, '847', 'second_yellow_cards')
    assert second_yellow.value is None
    assert second_yellow.observation_status == 'explicit_none' and second_yellow.raw_value == '-'
    assert aggregate(result, '847', 'average_second_yellow_cards').value is None
    assert aggregate(result, '847', 'direct_red_cards').value == Decimal('2')
    assert aggregate(result, '847', 'average_direct_red_cards').value == Decimal('0.06')
    assert aggregate(result, '847', 'penalties_awarded').value == Decimal('6')
    assert aggregate(result, '847', 'average_penalties_awarded').value == Decimal('0.19')
    # "Age at debut" is read past: it is not an aggregate.
    assert {row.metric for row in result.aggregates} == set(REFEREE_METRICS)


@pytest.mark.parametrize('old,new', [
    ('title="Penalty kicks"', 'title="Fouls"'),
    ('&oslash;-<span class="icons_sprite icon-gelbekarte-table-header"', '<span class="icons_sprite icon-gelbekarte-table-header"'),
    ('<th class="rechts" id="yw1_c2">', '<th class="rechts" id="yw1_c2">x</th><th class="rechts" id="yw1_c2">'),
    ('title="Appearances"', ''),
])
def test_unknown_or_repeated_icon_header_fails_closed(old, new):
    with pytest.raises(RefereeParseError, match='directory_headers_changed'):
        parse_referee_directory_page(
            DIRECTORY_ICONS.replace(old, new), competition_id='GB1', edition_id='2025',
        )


def test_compact_history_preserves_seasons_without_inventing_averages():
    result = parse_referee_season_aggregates(
        HISTORY_COMPACT, referee_id='101', competition_id='xy1',
    )
    assert len(result.aggregates) == 2 * len(REFEREE_COUNT_METRICS)
    assert not [row for row in result.aggregates if row.metric in REFEREE_AVERAGE_METRICS]
    assert {row.edition_id for row in result.aggregates} == {'2024', '2026'}
    assert aggregate(result, '101', 'yellow_cards', '2024').value == Decimal('61')
    second_yellow = aggregate(result, '101', 'second_yellow_cards', '2026')
    assert second_yellow.value is None
    assert second_yellow.observation_status == 'explicit_none'


def test_detailed_history_accepts_comma_decimal_and_keeps_competition():
    result = parse_referee_season_aggregates(
        HISTORY_DETAILED, referee_id=101, competition_id='XY1',
    )
    assert len(result.aggregates) == len(REFEREE_COUNT_METRICS) + len(REFEREE_AVERAGE_METRICS)
    row = aggregate(result, '101', 'average_yellow_cards', '2024')
    assert row.value == Decimal('4.36')
    assert row.competition_id == 'XY1' and row.raw_value == '4,36'


@pytest.mark.parametrize('raw,expected', [('1999/00', '1999'), ('2026', '2026')])
def test_history_supports_century_rollover_and_calendar_years(raw, expected):
    row = outer_row(HISTORY_COMPACT).replace('24/25', raw)
    html = f'<table class="items"><tbody>{row}</tbody></table>'
    result = parse_referee_season_aggregates(html, referee_id='101', competition_id='XY1')
    assert expected in {row.edition_id for row in result.aggregates}


@pytest.mark.parametrize('raw', ['24/27', '25', '20x6', '2025/2027'])
def test_inconsistent_or_unknown_season_fails_closed(raw):
    with pytest.raises(RefereeParseError, match='season_invalid'):
        parse_referee_season_aggregates(
            HISTORY_COMPACT.replace('24/25', raw), referee_id='101', competition_id='XY1',
        )


def test_competition_context_is_checked_against_every_history_row():
    with pytest.raises(RefereeParseError, match='competition_mismatch'):
        parse_referee_season_aggregates(
            HISTORY_COMPACT.replace('wettbewerb/XY1', 'wettbewerb/GB1'),
            referee_id='101', competition_id='XY1',
        )


def test_empty_metric_cell_is_observed_empty_not_numeric_zero():
    html = DIRECTORY_COMPACT.replace('<td class="zentriert">4</td>', '<td class="zentriert"></td>')
    result = parse_referee_directory_page(html, competition_id='XY1', edition_id='2025')
    row = aggregate(result, '101', 'penalties_awarded')
    assert row.value is None
    assert row.observation_status == 'observed_empty'
    assert row.status_reason == 'empty_cell' and row.raw_value == ''


@pytest.mark.parametrize('value', ['sixty', '-1', '1.5', '1,5', 'NaN'])
def test_malformed_or_invalid_count_fails_whole_capture(value):
    html = DIRECTORY_COMPACT.replace('<td class="zentriert">61</td>', f'<td class="zentriert">{value}</td>')
    with pytest.raises(RefereeParseError, match='metric_value_invalid'):
        parse_referee_directory_page(html, competition_id='XY1', edition_id='2025')


@pytest.mark.parametrize('html,reason', [
    ('', 'invalid_html'),
    ('<html>blocked</html>', 'referee_table_missing'),
    (DIRECTORY_COMPACT + DIRECTORY_COMPACT, 'referee_table_ambiguous'),
    ('<table class="items"><tbody></tbody></table>', 'empty_layout_unproven'),
    (DIRECTORY_DETAILED.replace('Yellow cards', 'Bookings'), 'directory_headers_changed'),
])
def test_unknown_or_unproven_directory_layout_fails_closed(html, reason):
    with pytest.raises(RefereeParseError, match=reason):
        parse_referee_directory_page(html, competition_id='XY1', edition_id='2025')


def test_malformed_referee_identity_fails_instead_of_dropping_row():
    html = DIRECTORY_COMPACT.replace('/profil/schiedsrichter/101', '/profil/schiedsrichter/broken')
    with pytest.raises(RefereeParseError, match='referee_identity_invalid'):
        parse_referee_directory_page(html, competition_id='XY1', edition_id='2025')


def test_profile_name_and_birth_date_validation():
    with pytest.raises(RefereeParseError, match='profile_identity_missing'):
        parse_referee_profile('<html></html>', referee_id='101')
    with pytest.raises(RefereeParseError, match='profile_birth_date_invalid'):
        parse_referee_profile(PROFILE.replace('10/03/1990', '1990-31-31'), referee_id='101')


def test_identical_directory_duplicates_are_reconciled_and_conflicts_fail():
    row = outer_row(DIRECTORY_COMPACT)
    duplicate = DIRECTORY_COMPACT.replace('</tbody>', row + '</tbody>')
    result = parse_referee_directory_page(duplicate, competition_id='XY1', edition_id='2025')
    assert len(result.referees) == 2 and len(result.aggregates) == 10
    assert result.reconciled_duplicates == 1

    conflict = DIRECTORY_COMPACT.replace(
        '</tbody>', row.replace('>61</td>', '>60</td>') + '</tbody>',
    )
    with pytest.raises(RefereeParseError, match='directory_conflict'):
        parse_referee_directory_page(conflict, competition_id='XY1', edition_id='2025')


def test_identical_history_duplicates_are_reconciled_and_conflicts_fail():
    row = outer_row(HISTORY_COMPACT)
    duplicate = HISTORY_COMPACT.replace('</tbody>', row + '</tbody>')
    result = parse_referee_season_aggregates(duplicate, referee_id='101', competition_id='XY1')
    assert len(result.aggregates) == 10 and result.reconciled_duplicates == 1

    conflict = HISTORY_COMPACT.replace(
        '</tbody>', row.replace('>61</td>', '>60</td>') + '</tbody>',
    )
    with pytest.raises(RefereeParseError, match='aggregate_conflict'):
        parse_referee_season_aggregates(conflict, referee_id='101', competition_id='XY1')


def test_complete_pages_merge_without_losing_counts_or_published_averages():
    result = reconcile_referee_directory_pages(
        [DIRECTORY_COMPACT, DIRECTORY_DETAILED],
        competition_id='XY1', edition_id='2025', expected_page_count=2,
    )
    assert len(result.referees) == 2
    assert len(result.aggregates) == 14
    assert result.reconciled_duplicates == 1
    assert aggregate(result, '101', 'average_yellow_cards').value == Decimal('4.36')
    assert aggregate(result, '202', 'yellow_cards').value == Decimal('29')


@pytest.mark.parametrize('pages,expected', [([], 0), ([DIRECTORY_COMPACT], 2), ([DIRECTORY_COMPACT], 0)])
def test_incomplete_or_invalid_pagination_never_claims_complete_discovery(pages, expected):
    with pytest.raises(RefereeParseError, match='directory_pagination_incomplete'):
        reconcile_referee_directory_pages(
            pages, competition_id='XY1', edition_id='2025', expected_page_count=expected,
        )


def test_single_complete_page_and_repeated_identical_pages_are_deterministic():
    single = reconcile_referee_directory_pages(
        [DIRECTORY_COMPACT], competition_id='XY1', edition_id='2025', expected_page_count=1,
    )
    repeated = reconcile_referee_directory_pages(
        [DIRECTORY_COMPACT, DIRECTORY_COMPACT],
        competition_id='XY1', edition_id='2025', expected_page_count=2,
    )
    assert len(single.aggregates) == len(repeated.aggregates) == 10
    assert repeated.reconciled_duplicates == 2
    assert repeated.canonical_json() == reconcile_referee_directory_pages(
        [DIRECTORY_COMPACT, DIRECTORY_COMPACT],
        competition_id='XY1', edition_id='2025', expected_page_count=2,
    ).canonical_json()


def test_replay_is_deterministic_and_decimal_json_is_safe():
    first = parse_referee_season_aggregates(
        HISTORY_DETAILED, referee_id='101', competition_id='XY1',
    )
    second = parse_referee_season_aggregates(
        HISTORY_DETAILED, referee_id='101', competition_id='XY1',
    )
    assert first == second and first.canonical_json() == second.canonical_json()
    payload = json.loads(first.canonical_json())
    assert payload['parser_revision'] == REFEREE_PARSER_REVISION
    assert payload['schema_revision'] == REFEREE_SCHEMA_REVISION
    assert payload['aggregates'][0]['value'] is not None


def test_empty_results_require_explicit_completeness_proof():
    directory = confirmed_empty_referee_directory(
        competition_id='XY1', edition_id='2025',
        completeness_proof='synthetic:full-directory-empty-v1',
    )
    history = confirmed_empty_referee_season_aggregates(
        referee_id='101', competition_id='XY1',
        completeness_proof='synthetic:full-history-empty-v1',
    )
    assert directory.referees == () and directory.aggregates == ()
    assert history.aggregates == ()
    assert directory.completeness_proof == 'synthetic:full-directory-empty-v1'
    assert history.completeness_proof == 'synthetic:full-history-empty-v1'
    with pytest.raises(RefereeParseError, match='completeness_proof_missing'):
        confirmed_empty_referee_directory(
            competition_id='XY1', edition_id='2025', completeness_proof='',
        )
    with pytest.raises(RefereeParseError, match='completeness_proof_missing'):
        confirmed_empty_referee_season_aggregates(
            referee_id='101', competition_id='XY1', completeness_proof='',
        )


def test_output_contract_has_long_grain_and_no_match_fields():
    result = parse_referee_directory_page(
        DIRECTORY_DETAILED, competition_id='XY1', edition_id='2025',
    )
    forbidden = {'match_id', 'game_id', 'home_team', 'away_team', 'event', 'appointment'}
    for row in result.aggregates:
        assert tuple(row.as_dict()) == REFEREE_AGGREGATE_COLUMNS
        assert not forbidden & set(row.as_dict())
        assert row.metric in REFEREE_COUNT_METRICS + REFEREE_AVERAGE_METRICS
    assert REFEREE_AGGREGATE_OBSERVATION_KEY == (
        'referee_id', 'competition_id', 'edition_id', 'metric', 'observed_at',
    )
    assert REFEREE_OBSERVATION_STATUSES == {
        'observed_present', 'observed_empty', 'explicit_none', 'not_applicable',
        'unknown', 'unavailable', 'error',
    }
