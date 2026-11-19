"""The host watch consumes completion evidence from the current writer."""
import json
import os
from datetime import datetime, timedelta, timezone

import pytest

from deploy.transfermarkt.transfermarkt_current_activity import has_recent_completion


pytestmark = pytest.mark.unit
NOW = datetime(2026, 11, 19, 15, tzinfo=timezone.utc)


def write_cursor(directory, roster_at=None):
    cursor = {'scope_id': 's', 'roster_completed_at': roster_at, 'last_work_at': NOW.isoformat()}
    (directory / 'cursor-v1.json').write_text(json.dumps({'scopes': {'s': {'cursor': cursor}}}))


def write_portion(directory, at, ids, status='pending'):
    path = directory / 'portion-test.json'
    path.write_text(json.dumps({'finished_at': at.isoformat(), 'status': status,
                                'daily_complete_scope_ids': ids}))
    # Deliberately make the file itself look new even if all evidence is old.
    os.utime(path, (NOW.timestamp(), NOW.timestamp()))


@pytest.mark.parametrize('status', ['pending', 'failed', 'blocked'])
def test_verified_daily_scope_in_a_finished_portion_keeps_current_alive(tmp_path, status):
    write_cursor(tmp_path)
    write_portion(tmp_path, NOW - timedelta(hours=24) + timedelta(seconds=1), ['s'], status)
    assert has_recent_completion(tmp_path, NOW) is True


def test_committed_roster_keeps_current_alive_without_a_daily_completion(tmp_path):
    write_cursor(tmp_path, (NOW - timedelta(hours=23)).isoformat())
    write_portion(tmp_path, NOW, [])
    assert has_recent_completion(tmp_path, NOW) is True


@pytest.mark.parametrize('age', [timedelta(hours=24), timedelta(hours=25), -timedelta(hours=1)])
def test_stalled_at_24h_even_if_files_and_last_work_are_recent(tmp_path, age):
    write_cursor(tmp_path, (NOW - age).isoformat())
    write_portion(tmp_path, NOW - age, ['s'])
    assert has_recent_completion(tmp_path, NOW) is False


def test_recent_portion_without_a_completed_scope_is_not_activity(tmp_path):
    write_cursor(tmp_path)
    write_portion(tmp_path, NOW, [])
    assert has_recent_completion(tmp_path, NOW) is False


def test_unavailable_evidence_cannot_clear_a_stall_but_one_positive_proof_can(tmp_path):
    assert has_recent_completion(tmp_path, NOW) is None
    write_cursor(tmp_path)
    path = tmp_path / 'portion-test.json'
    path.write_text('{')
    os.utime(path, (NOW.timestamp(), NOW.timestamp()))
    assert has_recent_completion(tmp_path, NOW) is None
    write_cursor(tmp_path, NOW.isoformat())
    assert has_recent_completion(tmp_path, NOW) is True
    (tmp_path / 'cursor-v1.json').write_text('{}')
    write_portion(tmp_path, NOW, ['s'])
    assert has_recent_completion(tmp_path, NOW) is True


def test_known_completion_is_usable_despite_a_broken_peer_cursor(tmp_path):
    scopes = {'bad': {'cursor': {}}, 's': {'cursor': {'scope_id': 's', 'roster_completed_at': NOW.isoformat()}}}
    (tmp_path / 'cursor-v1.json').write_text(json.dumps({'scopes': scopes}))
    assert has_recent_completion(tmp_path, NOW) is True
