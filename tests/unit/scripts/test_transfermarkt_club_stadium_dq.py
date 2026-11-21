from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json

import pandas as pd
import pytest

from dags.scripts.transfermarkt_club_stadium_dq import evaluate_dq, load_universe

NOW = datetime(2026, 11, 20, tzinfo=timezone.utc)


def universe():
    return {
        'scope_count': 1, 'enumerated_scope_count': 1, 'enumeration_complete': True,
        'expected_club_count': 2, 'clubs': {'1': ['scope'], '2': ['scope']},
        'scopes': {'scope': {
            'status': 'listed', 'expected_club_ids': ['1', '2'], 'pending_club_ids': [],
            'deferred': {}, 'completed': {}, 'sweep_started_at': NOW.isoformat(),
            'sweep_completed_at': NOW.isoformat(),
        }},
        'errors': [],
    }


def receipt(club, status='observed_present', at=NOW):
    return {
        'club_id': club, 'receipt_id': 'r' + club, 'stadium_status': status,
        'stadium_observed_at': at.isoformat(), 'batch_id': 'b' + club,
        'scope_id': 'scope', 'run_id': 'run', 'universe_version': 'universe',
    }


def query_for(rows, violation=None):
    def query(sql):
        if 'SELECT club_id, receipt_id, stadium_status' in sql:
            return pd.DataFrame(rows)
        value = int(violation is not None and violation in sql)
        return pd.DataFrame([[value]])
    return query


def test_green_present_and_proven_empty_coverage():
    report = evaluate_dq(query_for([receipt('1'), receipt('2', 'observed_empty')]), universe(), now=NOW)
    assert report['passed']
    assert report['club_conclusive_coverage'] == 1
    assert report['fresh_present_clubs'] == 1
    assert report['fresh_empty_clubs'] == 1
    assert report['errors'] == []


def test_stale_or_missing_observation_is_unresolved_not_empty():
    rows = [receipt('1', at=NOW - timedelta(days=31))]
    report = evaluate_dq(query_for(rows), universe(), now=NOW)
    assert not report['passed']
    assert report['unresolved_clubs'] == 2
    assert 'club_conclusive_coverage_below_95' in report['errors']
    assert 'scope_unresolved_above_5' in report['errors']


@pytest.mark.parametrize('violation', [
    'HAVING COUNT(*) > 1', 'raw_capture_id IS NULL', 's.club_id IS NULL',
    'expected_profile_count <>',
])
def test_physical_integrity_failure_blocks_pass(violation):
    report = evaluate_dq(
        query_for([receipt('1'), receipt('2')], violation), universe(), now=NOW)
    assert not report['passed']
    assert report['physical']['errors']


def test_unattempted_registry_scope_cannot_be_hidden_by_written_clubs():
    value = universe()
    value.update(scope_count=2, enumerated_scope_count=1, enumeration_complete=False,
                 expected_club_count=None)
    value['scopes']['other'] = {
        'status': 'unattempted', 'expected_club_ids': [], 'pending_club_ids': [], 'deferred': {},
    }
    report = evaluate_dq(query_for([receipt('1'), receipt('2')]), value, now=NOW)
    assert not report['passed']
    assert 'enumeration_incomplete' in report['errors']
    assert report['club_conclusive_coverage'] is None


def test_current_journal_universe_preserves_expected_clubs_and_deferred(tmp_path):
    scope_id = 'scope'
    relative = 'scopes/' + hashlib.sha256(scope_id.encode()).hexdigest() + '.json'
    path = tmp_path / relative
    path.parent.mkdir()
    path.write_text(json.dumps({
        'snapshot': {'expected_team_ids': ['1', '2']},
        'listing_clubs': {'1': {}, '2': {}},
        'stadium_sweep': {
            'started_at': NOW.isoformat(), 'pending': ['2'], 'deferred': {'2': 'blocked'},
            'completed': {'1': {'receipt_id': 'r1', 'status': 'observed_present'}},
        },
    }))
    (tmp_path / 'cursor-v1.json').write_text(json.dumps({
        'current_targets': [{'scope_id': scope_id}],
        'scopes': {scope_id: {'scope_file': relative}},
    }))
    result = load_universe(tmp_path)
    assert result['enumeration_complete']
    assert result['expected_club_count'] == 2
    assert result['scopes'][scope_id]['status'] == 'partial'
    assert result['scopes'][scope_id]['deferred'] == {'2': 'blocked'}
