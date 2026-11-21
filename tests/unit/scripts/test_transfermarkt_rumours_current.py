import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dags.scripts import run_transfermarkt_current as current
from dags.utils import transfermarkt_rumours as writer
from scrapers.transfermarkt import rumour_observations, rumours


BASE = 'https://www.transfermarkt.com/-/geruechte/wettbewerb/GB1'
SECOND = BASE + '/page/2'


@pytest.fixture
def run_feed(monkeypatch):
    context = rumours.CompetitionContext('GB1', 'Example league', BASE)
    scope = SimpleNamespace(
        target=SimpleNamespace(scope_id='gb1-scope', registry_snapshot_id='registry-v1'),
        scraper=SimpleNamespace(_current_child_cycle_id='child-cycle'),
        now=lambda: 'extraction-clock', cycle_id='parent-cycle', preflight=object(), check=Mock(),
    )
    read = Mock(side_effect=[
        ('first page', 'raw-1', 'hash-1', 'capture-clock-1'),
        ('second page', 'raw-2', 'hash-2', 'capture-clock-2'),
    ])

    def fetch(url, label, *, operation):
        assert (label, operation) == ('rumour_listing', 'rumours')
        html, raw, digest, captured = read(url)
        return SimpleNamespace(html=html, digest=digest, raw_capture_id=raw, raw_fetched_at=captured)

    scope.fetch = fetch
    pagination = Mock(side_effect=[
        SimpleNamespace(current_page=1, last_page=2, links=((2, SECOND),)),
        SimpleNamespace(current_page=2, last_page=2, links=()),
    ])
    extract = Mock(side_effect=lambda html, **lineage: lineage)
    write = Mock(return_value={
        'verified': True, 'receipt_readback': True, 'receipts': ['receipt'],
        'plan_sha256': 'verified-plan', 'reconciliation': {
            'source_rows': 3, 'retained_assertions': 3, 'status_counts': {'observed': 3},
        },
    })
    exclude = Mock(return_value=[(0,)])
    monkeypatch.setattr(rumours, 'parse_rumour_pagination', pagination)
    monkeypatch.setattr(rumour_observations, 'extract_rumour_page', extract)
    monkeypatch.setattr(rumour_observations, 'RumourFeedExtraction', tuple)
    monkeypatch.setattr(writer, 'write_rumours', write)
    monkeypatch.setattr(rumour_observations, 'rumour_exclusion_sql', lambda: 'exclusion SQL')

    def run():
        current._Scope._check_rumour_feed(
            scope, 'GB1', context, lambda page: (page.html, page.digest), exclude,
        )

    return SimpleNamespace(run=run, scope=scope, read=read, pagination=pagination,
                           extract=extract, write=write, exclude=exclude)


def test_full_feed_written_once_with_all_raw_lineage(run_feed):
    f = run_feed
    f.run()
    assert [call.args for call in f.read.call_args_list] == [(BASE,), (SECOND,)]
    f.write.assert_called_once()
    scraper, pages, preflight = f.write.call_args.args
    assert scraper is f.scope.scraper and preflight is f.scope.preflight
    assert [p['page_number'] for p in pages] == [1, 2]
    assert [p['captured_at'] for p in pages] == ['capture-clock-1', 'capture-clock-2']
    assert [p['source_body_hash'] for p in pages] == ['hash-1', 'hash-2']
    for page in pages:
        assert page['run_id'] == 'parent-cycle'
        assert page['cycle_id'] == 'child-cycle'
        assert page['scope_id'] == 'gb1-scope'
        assert page['universe_version'] == 'registry-v1'
    f.exclude.assert_called_once_with('exclusion SQL')
    f.scope.check.assert_called_once()
    assert f.scope.check.call_args.args == ('rumours', ('raw-1', 'raw-2'), 3, 3)
    proof = json.loads(f.scope.check.call_args.kwargs['result'])
    assert (proof['plan_sha256'], proof['pages'], proof['rumour_violations']) == ('verified-plan', 2, 0)


def test_second_fetch_failure_never_writes_partial_feed(run_feed):
    f = run_feed
    f.read.side_effect = [('first page', 'raw-1', 'hash-1', 'clock'), OSError('page 2')]
    with pytest.raises(OSError, match='page 2'):
        f.run()
    f.write.assert_not_called()
    f.scope.check.assert_not_called()


def test_changed_last_page_never_writes_partial_feed(run_feed):
    f = run_feed
    f.pagination.side_effect = [
        SimpleNamespace(current_page=1, last_page=2, links=((2, SECOND),)),
        SimpleNamespace(current_page=2, last_page=3, links=()),
    ]
    with pytest.raises(ValueError, match='pagination'):
        f.run()
    f.write.assert_not_called()
    f.scope.check.assert_not_called()


def test_repeat_and_corrected_bodies_go_through_writer(run_feed):
    f = run_feed
    f.pagination.side_effect = None
    f.pagination.return_value = SimpleNamespace(current_page=1, last_page=1, links=())
    f.read.side_effect = [
        ('initial', 'raw-1', 'first-hash', 'clock-1'),
        ('initial', 'raw-2', 'first-hash', 'clock-2'),
        ('corrected', 'raw-3', 'corrected-hash', 'clock-3'),
    ]
    for _ in range(3):
        f.run()
    pages = [call.args[1][0] for call in f.write.call_args_list]
    assert [page['raw_capture_id'] for page in pages] == ['raw-1', 'raw-2', 'raw-3']
    assert [page['source_body_hash'] for page in pages] == ['first-hash', 'first-hash', 'corrected-hash']
    assert f.scope.check.call_count == 3


@pytest.mark.parametrize('links', [(), ((2, BASE),), ((2, SECOND.replace('GB1', 'DE1')),)])
def test_invalid_next_page_aborts_before_write(run_feed, links):
    f = run_feed
    f.pagination.side_effect = [SimpleNamespace(current_page=1, last_page=2, links=links)]
    with pytest.raises(ValueError):
        f.run()
    f.read.assert_called_once_with(BASE)
    f.write.assert_not_called()
    f.scope.check.assert_not_called()


@pytest.mark.parametrize('fault', ['writer', 'verified', 'receipts', 'receipt_readback', 'exclusion', 'query'])
def test_failed_proof_never_completes(run_feed, fault):
    f = run_feed
    if fault == 'writer':
        f.write.side_effect = RuntimeError('writer')
    elif fault == 'query':
        f.exclude.side_effect = RuntimeError('query')
    elif fault == 'exclusion':
        f.exclude.return_value = [(1,)]
    else:
        f.write.return_value[fault] = False
    with pytest.raises((RuntimeError, ValueError)):
        f.run()
    f.scope.check.assert_not_called()


@pytest.mark.parametrize('raw_id,receipts,complete', [('raw-1', ['receipt'], True),
                                                       ('', ['receipt'], False), ('raw-1', [], False)])
def test_empty_feed_requires_raw_and_receipt(run_feed, raw_id, receipts, complete):
    f = run_feed
    f.read.side_effect = [('empty', raw_id, 'hash-1', 'clock')]
    f.pagination.side_effect = [SimpleNamespace(current_page=1, last_page=1, links=())]
    f.write.return_value['reconciliation'] = dict(source_rows=0, retained_assertions=0, status_counts={})
    f.write.return_value['receipts'] = receipts
    if complete:
        f.run()
        assert f.scope.check.call_args.args == ('rumours', ('raw-1',), 0, 0)
    else:
        with pytest.raises(ValueError):
            f.run()
        f.scope.check.assert_not_called()


@pytest.mark.parametrize('rows', [[], [(0,), (0,)], [(0, 0)], [(None,)], [('0',)], [(False,)]])
def test_exclusion_requires_one_numeric_scalar(rows):
    with pytest.raises(ValueError):
        current._check_rumour_exclusion(rows)


@pytest.mark.parametrize('query_failure', [False, True])
def test_public_rumours_decodes_fetch_and_closes_exclusion_query(monkeypatch, query_failure):
    scope = object.__new__(current._Scope)
    scope.initialize_resume = Mock()
    scope.target = SimpleNamespace(competition_id='GB1')
    scope.scope = {'record': SimpleNamespace(
        team_type=SimpleNamespace(value='club'), name='Example league', source_url=BASE,
    )}
    connection = Mock()
    cursor = connection.cursor.return_value
    cursor.fetchall.return_value = [(0,)]
    execute = Mock(side_effect=RuntimeError('query failed') if query_failure else None)
    monkeypatch.setattr(current, 'execute_statement', execute)
    scope.scraper = SimpleNamespace(_bronze_connection=Mock(return_value=connection))

    def check_feed(competition_id, context, decode_page, exclusion_rows):
        scope.initialize_resume.assert_called_once_with()
        assert competition_id == 'GB1'
        assert context == rumours.CompetitionContext(id='GB1', name='Example league', url=BASE)
        outcome = SimpleNamespace(value='<html></html>', raw_body_hash='body-sha256')
        assert decode_page(outcome) == (outcome.value, outcome.raw_body_hash)
        assert exclusion_rows('exclusion SQL') == [(0,)]

    scope._check_rumour_feed = Mock(side_effect=check_feed)
    if query_failure:
        with pytest.raises(RuntimeError, match='query failed'):
            scope.rumours()
        cursor.fetchall.assert_not_called()
    else:
        scope.rumours()
        cursor.fetchall.assert_called_once_with()
    scope._check_rumour_feed.assert_called_once()
    execute.assert_called_once_with(cursor, 'exclusion SQL')
    cursor.close.assert_called_once_with()
    connection.close.assert_called_once_with()


@pytest.mark.parametrize('listing_status,raws,has_clock,complete', [
    (None, [], False, False),
    ('partial', ['raw-listing'], True, False),
    ('complete', [], True, False),
    ('complete', ['raw-listing'], False, False),
    ('complete', ['raw-listing'], True, True),
])
def test_public_national_rumours_requires_complete_listing(listing_status, raws, has_clock, complete):
    scope = object.__new__(current._Scope)
    scope.initialize_resume = Mock()
    scope.scope = {'record': SimpleNamespace(team_type=SimpleNamespace(value='national_team'))}
    scope.target = SimpleNamespace(registry_snapshot_id='registry-v1')
    scope.check = Mock()
    scope.fetch = Mock()
    checked_at = '2026-11-20T06:00:00+00:00'
    listing = {'status': listing_status, 'raw_capture_ids': raws, 'checked_at': checked_at if has_clock else None}
    scope.data = {'checks': {'listing': listing}} if listing_status else {}
    if complete:
        scope.rumours()
        assert scope.check.call_args.args == ('rumours', ['raw-listing'], 0, 0)
        assert scope.check.call_args.kwargs['checked_at'] == current._utc(checked_at)
        assert json.loads(scope.check.call_args.kwargs['result'])['team_type'] == 'national_team'
    else:
        with pytest.raises(current.CurrentPortionError):
            scope.rumours()
        scope.check.assert_not_called()
    scope.initialize_resume.assert_called_once_with()
    scope.fetch.assert_not_called()


def test_national_check_uses_listing_evidence_without_fetching():
    scope = SimpleNamespace(target=SimpleNamespace(registry_snapshot_id='registry-v1'), check=Mock())
    current._Scope._check_national_rumours(scope, ['raw-listing'], 'listing-clock', 'national')
    scope.check.assert_called_once()
    assert scope.check.call_args.args == ('rumours', ['raw-listing'], 0, 0)
    assert scope.check.call_args.kwargs['checked_at'] == 'listing-clock'
    assert json.loads(scope.check.call_args.kwargs['result']) == {
        'applicability_status': 'not_applicable',
        'rule': 'league_rumour_listing:club_competitions',
        'team_type': 'national',
        'registry_snapshot_id': 'registry-v1',
    }


@pytest.mark.parametrize('raws,clock', [([], 'clock'), ([''], 'clock'), (['raw'], None)])
def test_national_check_requires_listing_evidence(raws, clock):
    scope = SimpleNamespace(check=Mock())
    with pytest.raises(ValueError, match='listing evidence'):
        current._Scope._check_national_rumours(scope, raws, clock, 'national')
    scope.check.assert_not_called()
