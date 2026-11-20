from __future__ import annotations

import json

import pytest

from dags.scripts import run_transfermarkt_current as current
from tests.unit.scripts import test_run_transfermarkt_current as current_fixtures
from tests.unit.dags.test_transfermarkt_scope_planner import _competition, _edition, _joined_row

offline = current_fixtures.offline
production_current = current_fixtures.production_current

pytestmark = pytest.mark.unit


def attendance_page(club_ids, *, next_url=None):
    rows = []
    for index, club_id in enumerate(club_ids, 1):
        rows.append(
            f'<tr class="odd"><td>{index}</td><td><table class="inline-table">'
            f'<tr><td><a href="/club-{club_id}/stadion/verein/{club_id}">Stadium {club_id}</a></td></tr>'
            f'<tr><td><a href="/club-{club_id}/startseite/verein/{club_id}">Club {club_id}</a></td></tr>'
            f'</table></td><td>10.000</td><td>100.000</td><td>5.000</td>'
            f'<td>20</td><td>0</td><td>50,0 %</td></tr>'
        )
    navigation = f'<a rel="next" href="{next_url}">next</a>' if next_url else ''
    return (
        '<html><table class="items"><thead><tr><th>#</th><th>Stadium</th><th>Capacity</th>'
        '<th>Spectators</th><th>Average</th><th>Matches</th><th>Sold out</th><th>Capacity</th>'
        f'</tr></thead><tbody>{"".join(rows)}</tbody></table>{navigation}</html>'
    )


def attendance_html(url, offline):
    custom = getattr(offline, 'attendance_pages', None)
    if custom is not None:
        return custom(url)
    return attendance_page(offline.club_ids)


def attendance_writer_for(offline, *, fail_once=False):
    calls = []

    def writer(scraper, extraction, expected_club_ids, preflight, **identity):
        calls.append((extraction, tuple(expected_club_ids), identity))
        if fail_once and len(calls) == 1:
            raise RuntimeError('attendance write failed')
        return {
            'verified': True,
            'receipt_readback': True,
            'bronze_manifest': extraction.batch_id,
            'committed_at': offline.clock.wall.isoformat(),
            'expected_club_ids': sorted(expected_club_ids, key=int),
            'observed_at': identity['observed_at'],
            'final_season': extraction.final_season,
        }

    return writer, calls


def data(offline):
    return next(iter(offline.state()['scopes'].values()))


def attendance_calls(offline):
    return [url for _, url in offline.calls if '/besucherzahlen/' in url]


def test_weekly_attendance_commits_edition_and_fresh_run_is_noop(offline):
    writer, calls = attendance_writer_for(offline)
    offline.run_cold('cold', attendance_writer=writer)
    assert len(calls) == 1
    extraction, expected, identity = calls[0]
    assert extraction.result.status == 'observed_present'
    assert expected == ('10', '20')
    assert identity['competition_id'] == 'GB1'
    assert identity['edition_id'] == '2026'
    assert data(offline)['cursor']['attendance_completed_at']
    assert not data(offline)['cursor']['attendance_pending']
    offline.run('fresh-repeat', attendance_writer=writer)
    assert len(calls) == 1


def test_attendance_runs_again_on_seventh_day(offline):
    writer, calls = attendance_writer_for(offline)
    offline.run_cold('cold', attendance_writer=writer)
    offline.clock.advance(6 * 86400)
    offline.run('six-days', attendance_writer=writer)
    assert len(calls) == 1
    offline.clock.advance(86400)
    offline.run('seven-days', attendance_writer=writer)
    assert len(calls) == 2


def test_broken_page_keeps_attendance_pending_and_retries_only_attendance(offline):
    offline.attendance_pages = lambda url: '<html>blocked</html>'
    writer, calls = attendance_writer_for(offline)
    offline.run('broken', attendance_writer=writer)
    assert not calls
    assert data(offline)['cursor']['attendance_pending']
    assert data(offline)['attendance_error']
    offline.attendance_pages = None
    offline.run('resume', attendance_writer=writer)
    assert len(calls) == 1
    assert not data(offline)['cursor']['attendance_pending']
    assert data(offline)['cursor']['attendance_completed_at']


def test_writer_failure_keeps_pending_and_recovers(offline):
    writer, calls = attendance_writer_for(offline, fail_once=True)
    offline.run('failed-write', attendance_writer=writer)
    assert data(offline)['cursor']['attendance_pending']
    offline.clock.advance(3600)
    offline.run('resume', attendance_writer=writer)
    assert len(calls) == 2
    assert not data(offline)['cursor']['attendance_pending']
    assert data(offline)['cursor']['attendance_completed_at'] == offline.clock.wall.isoformat()
    assert 'attendance_receipt' in data(offline)


def test_multi_page_capture_uses_saved_pages_and_one_observation(offline):
    start = 'https://www.transfermarkt.com/gb1/besucherzahlen/wettbewerb/GB1/saison_id/2026'
    second = start + '?page=2'
    def pages(url):
        offline.clock.advance(1)
        return attendance_page(('10',), next_url=second) if url == start else attendance_page(('20',))

    offline.attendance_pages = pages
    writer, calls = attendance_writer_for(offline)
    offline.run('multi-page', attendance_writer=writer)
    assert len(calls) == 1
    extraction, _, _ = calls[0]
    assert extraction.result.page_count == 2
    assert {row.club_id for row in extraction.result.rows} == {'10', '20'}
    assert len(attendance_calls(offline)) == 2
    assert len({row.observed_at for row in extraction.result.rows}) == 1
    assert len({row.captured_at for row in extraction.result.rows}) == 2


def test_old_cursor_migrates_additive_attendance_fields_without_losing_resume():
    cursor = current.ScopeCursor('scope', generation='old', resume_json='{"clubs": {}}')
    old = cursor.as_dict()
    old.pop('attendance_completed_at')
    old.pop('attendance_pending')
    migrated = current.ScopeCursor.from_mapping(old)
    assert migrated.attendance_completed_at is None
    assert migrated.attendance_pending is False
    assert migrated.resume_json == cursor.resume_json


def test_legacy_attendance_pending_is_migrated_from_resume():
    cursor = current.ScopeCursor('scope', generation='old').as_dict()
    cursor['resume_json'] = json.dumps({'attendance_pending': True, 'clubs': {}})
    migrated = current.ScopeCursor.from_mapping(cursor)
    assert migrated.attendance_pending is True
    assert json.loads(migrated.resume_json) == {'clubs': {}}


def close_report(offline, writer):
    offline.run('prepare')
    rows = [
        _joined_row(_competition('GB1'), _edition('GB1', '2026', current=False)),
        _joined_row(_competition('GB1'), _edition('GB1', '2027', current=True)),
    ]
    return current.run_current_portion(
        rows, {'paid_io_allowed': True, 'write_mode': 'dual', 'revision': 1}, 'close',
        denominator_rows=[{'competition_id': 'GB1', 'live': True, 'competition_class': 'core_club', 'tier': 1}],
        state_dir=offline.root / 'state', qualification=offline.qualification,
        max_seconds=2400, max_scopes=1, scraper_factory=offline.factory,
        roster_writer=offline.roster_writer, career_writer=offline.career_writer,
        coach_writer=offline.coach_writer, attendance_writer=writer,
        now_fn=lambda: offline.clock.wall, monotonic_fn=lambda: offline.clock.elapsed,
    )


def test_season_close_requires_and_commits_final_attendance(offline):
    writer, calls = attendance_writer_for(offline)
    report = close_report(offline, writer)
    result, = report['scopes']
    assert result['kind'] == 'season_close'
    assert result['status'] == 'complete'
    assert result.get('historical_handoff') is True
    assert len(calls) == 1
    extraction, _, _ = calls[0]
    assert extraction.final_season is True
    assert extraction.final_season_proof.startswith('season-close:')


def test_failed_final_attendance_blocks_historical_handoff(offline):
    writer, calls = attendance_writer_for(offline, fail_once=True)
    report = close_report(offline, writer)
    result, = report['scopes']
    assert result['status'] == 'pending'
    assert not result.get('historical_handoff')
    assert data(offline)['cursor']['attendance_pending']
    assert 'season_close_handed_off_at' not in data(offline)


def test_enabled_but_unavailable_attendance_blocks_historical_handoff(offline, monkeypatch):
    monkeypatch.setenv('TM_ATTENDANCE_ENABLED', 'true')
    report = close_report(offline, None)
    result, = report['scopes']
    assert result['kind'] == 'season_close'
    assert result['status'] == 'pending'
    assert not result.get('historical_handoff')
    assert 'attendance_final_receipt' not in data(offline)
    assert 'season_close_handed_off_at' not in data(offline)


def test_production_current_attendance_is_disabled_by_default(
        offline, production_current, monkeypatch):
    from dags.utils.transfermarkt_attendance import ATTENDANCE_RECEIPTS_TABLE
    from scrapers.transfermarkt.attendance import ATTENDANCE_TABLE

    policy = json.loads(production_current.policy_path.read_text())
    policy['allowed_write_tables'] = sorted(set(policy['allowed_write_tables']) | {
        ATTENDANCE_TABLE, ATTENDANCE_RECEIPTS_TABLE,
    })
    production_current.policy_path.write_text(json.dumps(policy))
    monkeypatch.delenv('TM_ATTENDANCE_ENABLED', raising=False)

    def unexpected_write(*args, **kwargs):
        raise AssertionError('disabled attendance writer must not run')

    monkeypatch.setattr(
        'dags.utils.transfermarkt_attendance.write_attendance_observations', unexpected_write)
    report = production_current.run('production-disabled')
    assert report['attendance_status'] == 'attendance disabled'
    assert attendance_calls(offline) == []
    assert data(offline)['cursor']['attendance_completed_at'] is None


def test_production_current_wires_enabled_attendance_writer(
        offline, production_current, monkeypatch):
    from dags.utils.transfermarkt_attendance import ATTENDANCE_RECEIPTS_TABLE
    from scrapers.transfermarkt.attendance import ATTENDANCE_TABLE

    policy = json.loads(production_current.policy_path.read_text())
    policy['allowed_write_tables'] = sorted(set(policy['allowed_write_tables']) | {
        ATTENDANCE_TABLE, ATTENDANCE_RECEIPTS_TABLE,
    })
    production_current.policy_path.write_text(json.dumps(policy))
    monkeypatch.setenv('TM_ATTENDANCE_ENABLED', 'true')
    writer, calls = attendance_writer_for(offline)
    monkeypatch.setattr(
        'dags.utils.transfermarkt_attendance.write_attendance_observations', writer)
    report = production_current.run('production-wiring')
    assert report.get('attendance_status') != 'attendance disabled'
    assert len(calls) == 1
    assert data(offline)['cursor']['attendance_completed_at']
