from dataclasses import replace
from pathlib import Path

import pytest

from scrapers.transfermarkt.club_values import (
    CLUB_VALUE_KEY, ClubValueCapture, ClubValueParseError,
    build_club_value_dq, parse_club_values, with_stichtag,
)


pytestmark = pytest.mark.unit
FIXTURES = Path(__file__).resolve().parents[2] / 'fixtures' / 'transfermarkt' / 'club_values'
HASH = 'a' * 64
CAPTURED = '2026-11-20T15:40:00+00:00'
URLS = {
    'vie1-current': 'https://www.transfermarkt.com/v-league-1/marktwerteverein/wettbewerb/VIE1',
    'vie1-2024-06-01': 'https://www.transfermarkt.com/v-league-1/marktwerteverein/wettbewerb/VIE1/stichtag/2024-06-01',
    'vie1-2013-04-15': 'https://www.transfermarkt.com/v-league-1/marktwerteverein/wettbewerb/VIE1/stichtag/2013-04-15',
}


def capture(url):
    return ClubValueCapture(
        competition_id='VIE1', source_url=url, raw_capture_id=HASH,
        source_body_hash=HASH, captured_at=CAPTURED, extracted_at='2026-11-20T16:00:00+00:00',
        scope_id='scope', run_id='run', cycle_id='cycle', universe_version='universe',
    )


def parse(name, html=None):
    return parse_club_values(html or (FIXTURES / (name + '.html')).read_text(), capture(URLS[name]))


def test_current_preserves_rounded_and_different_table_observations():
    result = parse('vie1-current')
    assert len(result.club_ids) == 14
    assert len(result.rows) == 32
    assert result.status_counts == {'observed_present': 32}
    assert result.reconciled_table_points == 24
    assert result.differing_table_points == 4
    assert result.value_dates == ('2026-11-01', '2026-11-15')
    assert not result.full_available_history
    ha_noi = [row for row in result.rows if row['club_name'] == 'Ha Noi FC' and row['value_date'] == '2026-11-15']
    assert len(ha_noi) == 2
    assert {row['value_amount'] for row in ha_noi} == {4_950_000, 4_975_000}
    assert {row['reconciliation_status'] for row in ha_noi} == {'value_differs'}
    assert len({row['source_point_id'] for row in ha_noi}) == 2
    assert len({tuple(row[column] for column in CLUB_VALUE_KEY) for row in result.rows}) == len(result.rows)


def test_history_chart_is_right_aligned_and_zero_is_not_a_club_value():
    result = parse('vie1-2013-04-15')
    assert len(result.rows) == 3499
    assert result.status_counts == {'explicit_none': 6, 'observed_present': 3204, 'unknown': 289}
    assert result.reconciled_table_points == 18
    assert result.differing_table_points == 2
    assert result.value_dates[0] == '2013-04-15'
    assert result.value_dates[-1] == '2026-11-15'
    assert not result.full_available_history
    bac_ninh = result.club_rows('116338')
    assert bac_ninh[0]['value_date'] == '2013-04-15'
    assert bac_ninh[0]['observation_status'] == 'explicit_none'
    assert bac_ninh[0]['value_amount'] is None
    graph = [row for row in bac_ninh if row['source_point_kind'] == 'highcharts']
    assert graph[0]['value_date'] == '2024-02-01'
    assert graph[0]['observation_status'] == 'unknown'
    assert graph[0]['value_amount'] is None
    assert {row['value_amount'] for row in bac_ninh if row['value_date'] == '2026-11-15'} == {1_250_000, 1_275_000}
    ca_hcmc = result.club_rows('20830')
    assert ca_hcmc[0]['value_date'] == '2013-04-15'
    assert ca_hcmc[0]['value_amount'] == 100_000
    assert ca_hcmc[1]['value_date'] == '2013-05-01'
    assert ca_hcmc[1]['value_amount'] == 100_000
    assert all(row['value_amount'] != 0 for row in result.rows)


def test_second_real_history_capture_reconciles_all_series():
    result = parse('vie1-2024-06-01')
    assert len(result.rows) == 857
    assert result.status_counts == {'explicit_none': 1, 'observed_present': 797, 'unknown': 59}
    assert result.reconciled_table_points == 25
    assert result.differing_table_points == 3
    assert all(len(result.club_rows(club_id)) >= 61 for club_id in result.club_ids)


def test_earliest_stichtag_marks_full_available_history_and_missing_statuses():
    html = '''<table class="items"><thead><tr><th>#</th><th>wappen</th><th>Club</th><th>League</th><th>Value 01/01/2020</th><th>Current value</th><th>%</th></tr></thead><tbody><tr><td>1</td><td></td><td><a href="/club/startseite/verein/10">Club</a></td><td>League</td><td>-</td><td>€150.0K</td><td>-</td></tr></tbody></table>
<select name="stichtag"><option value="2020-01-01" selected>01/01/2020</option><option value="2020-01-15">15/01/2020</option></select>
<script>Highcharts.Chart({'xAxis':{'categories':['01\\x2F01\\x2F2020','15\\x2F01\\x2F2020']},'series':[{'name':'Club','data':[{'y':0},{'y':150000}]}]})</script>'''
    result = parse_club_values(html, capture(URLS['vie1-current'].replace('/VIE1', '/VIE1/stichtag/2020-01-01')))
    assert result.full_available_history
    assert len(result.rows) == 2
    assert result.rows[0]['observation_status'] == 'explicit_none'
    assert result.rows[0]['value_amount'] is None
    assert result.rows[1]['observation_status'] == 'observed_present'
    assert result.rows[1]['value_amount'] == 150_000
    assert result.rows[1]['source_point_kind'] == 'highcharts+table_current'


@pytest.mark.parametrize('mutation,reason', [
    ('<html>blocked</html>', 'club_table_missing'),
    ('<table class="items"></table>', 'club_table_headers_changed'),
])
def test_empty_or_blocked_layout_is_not_observed_empty(mutation, reason):
    with pytest.raises(ClubValueParseError, match=reason):
        parse_club_values(mutation, capture(URLS['vie1-current']))


def test_club_history_candidate_url_is_not_accepted_as_competition_source():
    with pytest.raises(ClubValueParseError, match='source_url_competition_mismatch'):
        capture('https://www.transfermarkt.com/club/marktwertverlauf/verein/10')


def test_non_eur_value_fails_closed():
    html = (FIXTURES / 'vie1-current.html').read_text().replace('€6.45M', '£6.45M', 1)
    with pytest.raises(ClubValueParseError, match='money_locale_changed'):
        parse('vie1-current', html)


def test_chart_and_table_club_sets_must_match():
    html = (FIXTURES / 'vie1-current.html').read_text().replace("'name':'Cong\\x20An\\x20Ha\\x20Noi\\x20FC'", "'name':'Other Club'", 1)
    with pytest.raises(ClubValueParseError, match='chart_table_clubs_differ'):
        parse('vie1-current', html)


def test_dq_counts_lineage_duplicates_and_receipt_coverage():
    result = parse('vie1-current')
    dq = build_club_value_dq(result)
    assert dq['structural_passed']
    assert dq['expected_clubs'] == 14
    assert dq['expected_points'] == 32
    assert dq['duplicate_observations'] == 0
    assert dq['null_lineage_columns'] == []
    assert dq['differing_table_points'] == 4


def test_stichtag_url_uses_the_current_competition_endpoint():
    assert with_stichtag(URLS['vie1-current'], '2010-11-01') == URLS['vie1-current'] + '/stichtag/2010-11-01'
    with pytest.raises(ClubValueParseError, match='stichtag_base_url_invalid'):
        with_stichtag(URLS['vie1-2013-04-15'], '2010-11-01')


def test_extraction_rejects_mixed_capture_lineage():
    result = parse('vie1-current')
    rows = (dict(result.rows[0], raw_capture_id='b' * 64),) + tuple(result.rows[1:])
    with pytest.raises(ClubValueParseError, match='capture_mismatch:raw_capture_id'):
        replace(result, rows=rows)
