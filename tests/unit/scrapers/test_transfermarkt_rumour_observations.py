from dataclasses import replace
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from scrapers.transfermarkt.rumour_observations import (
    RUMOUR_COLUMNS, RUMOUR_KEY, RumourFeedExtraction, build_rumour_dq, canonical_json,
    extract_rumour_page, rumour_exclusion_sql,
)
from scrapers.transfermarkt.rumours import CompetitionContext, RumourParseError, parse_rumour_pagination


pytestmark = pytest.mark.unit
GB1 = CompetitionContext('GB1', 'Premier League', '/premier-league/startseite/wettbewerb/GB1')
FIXTURES = Path('tests/fixtures/transfermarkt/rumours')
CAPTURED = datetime(2026, 11, 20, 12, 0, 0, 123456, tzinfo=timezone.utc)
EXTRACTED = CAPTURED + timedelta(hours=1)


@lru_cache(maxsize=2)
def saved(number):
    return (FIXTURES / f'gb1-page-{number}.html').read_bytes()


@lru_cache(maxsize=2)
def listing_url(number):
    nav = parse_rumour_pagination(saved(number).decode(), competition=GB1)
    return dict(nav.links)[number]


def player_row(player, probability='52.5%'):
    return f'''<tr><td><a href="/player/profil/spieler/{player}">Player {player}</a>
        <span class="position">Forward</span></td><td><img title="France"/></td><td>23</td>
        <td><a href="/old/startseite/verein/12">Old Club</a></td>
        <td><a href="/new/startseite/verein/14">New Club</a>
        <a href="/other/startseite/wettbewerb/FR1">Other league</a></td>
        <td><a href="//thread/forum/154/thread_id/1208229">20/11/2026</a></td>
        <td>-</td><td>{probability}</td></tr>'''


@lru_cache(maxsize=16)
def listing(number, rows):
    # Keep the real pager's markup and produce a bounded, two-page listing.
    soup = BeautifulSoup(saved(number), 'html.parser')
    pager = soup.select_one('#yw1 .pager')
    for anchor in list(pager.select('a.tm-pagination__link[href]')):
        item = anchor.find_parent('li')
        classes = ' '.join(item.get('class', ()))
        label = anchor.get_text(strip=True)
        if 'last-page' in classes:
            anchor['href'] = listing_url(2)
            if label.isdecimal():
                anchor.string = '2'
        elif (label.isdecimal() and int(label) > 2) or (number == 2 and 'next-page' in classes):
            item.decompose()
    return f'''<div id="yw1"><table class="items"><thead><tr><th>Player</th><th>Nation</th>
        <th>Age</th><th>Current club</th><th>Interested club</th><th>Most recent source</th>
        <th>Last reply</th><th>Assessment</th></tr></thead><tbody>{rows}</tbody></table>
        {pager}</div>'''


def extract(number, html, **overrides):
    body = html if isinstance(html, bytes) else html.encode()
    body_hash = hashlib.sha256(body).hexdigest()
    raw_identity = [listing_url(number), str(overrides.get('captured_at', CAPTURED)), body_hash]
    values = dict(competition=GB1, page_number=number, source_page_url=listing_url(number),
                  raw_capture_id=hashlib.sha256(canonical_json(raw_identity).encode()).hexdigest(),
                  source_body_hash=body_hash, captured_at=CAPTURED,
                  extracted_at=EXTRACTED, scope_id='test-scope', run_id='test-run',
                  cycle_id='test-cycle', universe_version='test-registry')
    values.update(overrides)
    return extract_rumour_page(html, **values)


def feed(*, second='', probability='52.5%', **lineage):
    return RumourFeedExtraction((
        extract(1, listing(1, player_row(17, probability)), **lineage),
        extract(2, listing(2, second), **lineage),
    ))


def test_saved_page_retains_real_assertions_and_raw_identity():
    body = saved(1)
    page = extract(1, body)
    assert len(page.rows) + page.capture.duplicate_count == 25
    assert page.pagination.last_page == 5
    assert all(row['_entity_type'] == 'transfer_rumour_assertion' for row in page.rows)
    assert all(row['source_body_hash'] == hashlib.sha256(body).hexdigest() for row in page.rows)
    assert any(row['thread_link'] and row['last_source_date'] for row in page.rows)
    with pytest.raises((RumourParseError, ValueError)):
        RumourFeedExtraction((page,))


def test_complete_plan_replays_and_projects_the_parser_contract():
    result = feed(second=player_row(19, '-'))
    first, second = result.pages
    row, = first.rows
    assert tuple(row) == RUMOUR_COLUMNS
    assert RUMOUR_KEY == ('rumour_identity', 'subject_id', 'observed_at', 'correction_identity')
    assert row['player_id'] == '17' and row['subject_id'] == 'transfermarkt:player:17'
    assert row['current_club_id'] == '12' and row['interested_club_name'] == 'New Club'
    assert row['interested_club_competition_id'] == 'FR1' and row['position'] == 'Forward'
    assert row['nationalities_json'] == '["France"]' and row['age'] == 23
    assert row['last_source_date'] == '2026-11-20' and row['last_reply_date'] is None
    assert row['thread_link'].endswith('/thread/forum/154/thread_id/1208229')
    assert row['probability_percent'] == 52.5 and second.rows[0]['probability_percent'] is None
    assert row['assertion_json'] == canonical_json(first.capture.observations[0].to_dict())
    assert row['_source'] == 'transfermarkt' and row['_entity_type'] == 'transfer_rumour_assertion'
    assert row['captured_at'] == row['observed_at'] == '2026-11-20T12:00:00.123456Z'
    assert row['_batch_id'] != second.batch_id
    assert result.plan_sha256() == RumourFeedExtraction((second, first)).plan_sha256()
    assert result.plan_sha256() == feed(second=player_row(19, '-')).plan_sha256()
    assert result.plan_sha256() != RumourFeedExtraction((replace(first, raw_capture_id='a'*64),
                                                         second)).plan_sha256()
    assert result.plan_sha256() != feed().plan_sha256()
    assert not {'transfer_events', 'matches', 'match_id', 'transfer_id'}.intersection(row)
    assert json.loads(row['assertion_json'])['entity_type'] == 'transfer_rumour_assertion'


@pytest.mark.parametrize('change', [
    {'raw_capture_id': 'x'}, {'source_body_hash': 'a' * 64}, {'scope_id': ' '},
    {'run_id': ''}, {'cycle_id': None}, {'universe_version': ''},
    {'captured_at': CAPTURED.replace(tzinfo=None)}, {'extracted_at': '2026-11-20'},
    {'extracted_at': CAPTURED - timedelta(seconds=1)}, {'page_number': 2},
])
def test_invalid_lineage_is_atomic(change):
    with pytest.raises((RumourParseError, ValueError)):
        extract(1, listing(1, player_row(17)), **change)


def test_broken_rows_and_noncanonical_url_fail():
    with pytest.raises(RumourParseError):
        extract(1, listing(1, player_row(17) + player_row(12, 'invalid')))
    with pytest.raises((RumourParseError, ValueError)):
        extract(1, listing(1, player_row(17)), source_page_url=listing_url(1).replace('https:', 'http:'))


def test_missing_duplicate_and_inconsistent_pages_fail():
    one, two = feed().pages
    for pages in ((one,), (two,), (one, two, two), (one, replace(two, scope_id='different'))):
        with pytest.raises((RumourParseError, ValueError)):
            RumourFeedExtraction(pages)
    dq = build_rumour_dq((one,))
    assert (dq['expected_pages'], dq['terminal_pages']) == (2, 1)
    assert dq['fresh_conclusive'] == dq['unresolved'] == 1
    assert not dq['complete_page_plan'] and not dq['structural_passed']
    assert build_rumour_dq((one,), ())['reconciliation_passed'] is False
    missing = build_rumour_dq((), ())
    assert missing['expected_pages'] is None and missing['unresolved'] is None
    assert missing['fresh_conclusive'] == 0 and not missing['reconciliation_passed']


def test_page_status_counts_duplicates_and_null_lineage():
    row = player_row(17)
    one = extract(1, listing(1, row + row))
    two = extract(2, listing(2, ''))
    result = RumourFeedExtraction((one, two))
    dq = build_rumour_dq(result)
    assert dq['structural_passed'] and dq['complete_page_plan']
    assert dq['fresh_conclusive'] == 2 and dq['unresolved'] == 0
    assert dq['status_counts'] == {'observed_present': 1, 'observed_empty': 1}
    assert dq['source_rows'] == 2 and dq['retained_assertions'] == 1
    assert dq['distinct_subjects'] == dq['duplicate_count'] == dq['unique_natural_keys'] == 1
    assert dq['conflict_count'] == dq['null_required_lineage'] == 0
    assert dq['reconciliation_passed'] is None
    assert build_rumour_dq(result, ())['receipt_coverage'] == {1: False, 2: False}
    bad = build_rumour_dq((one, replace(two, raw_capture_id=None)))
    assert bad['null_required_lineage'] == 1 and not bad['structural_passed']
    assert bad['fresh_conclusive'] == bad['unresolved'] == 1


def test_cross_page_checks_use_all_four_key_fields():
    duplicate = build_rumour_dq(feed(second=player_row(17)))
    assert duplicate['duplicate_natural_keys'] == 1 and not duplicate['structural_passed']
    assert duplicate['conflict_count'] == 1  # Same key appears with two page lineages.
    corrected = build_rumour_dq(feed(second=player_row(17, '25%')))
    assert corrected['unique_natural_keys'] == 2 and corrected['structural_passed']
    assert corrected['distinct_subjects'] == 1 and corrected['retained_assertions'] == 2


def test_exclusion_query_counts_all_three_routes():
    sql = rumour_exclusion_sql()
    assert 'SELECT count(*)' in sql and 'iceberg.bronze.transfermarkt_transfer_events e' in sql
    assert "e.source_url LIKE '%/geruechte/%'" in sql
    assert "OR e.parser_revision LIKE 'transfermarkt-rumours/%'" in sql
    assert 'OR EXISTS' in sql and 'iceberg.ops.transfermarkt_rumour_receipts_v1 r' in sql
    assert 'r.source_body_hash = e.source_body_hash' in sql
