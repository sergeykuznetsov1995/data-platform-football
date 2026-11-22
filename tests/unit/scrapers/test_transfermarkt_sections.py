"""Stage-C section lanes: attendance (T10), club values (T11), achievements (T8) (#2405)."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import pytest

from scrapers.transfermarkt import core, native_writer, sections
from scrapers.transfermarkt.attendance import AttendancePage
from scrapers.transfermarkt.core import CoreParseError, LaneOutcome, ScopeContext, SourceCapture
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.native_writer import StaleTransfermarktWrite
from scrapers.transfermarkt.orchestrator import (
    AttendanceEditionTarget,
    ClubValueTarget,
    CoreRun,
    CoreRunError,
    CoreScopePlan,
    RumourCompetitionTarget,
    attendance_url,
    club_values_url,
    planned_urls,
)
from scrapers.transfermarkt.native_writer import NativeWritePlan
from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def isolated_paths(monkeypatch, tmp_path):
    """Keep the journaled writer out of the host's shared Airflow logs volume."""
    monkeypatch.setenv('TM_WRITER_LOCK_PATH', str(tmp_path / 'writer.lock'))
    monkeypatch.setenv('TM_CORE_WRITE_JOURNAL_DIR', str(tmp_path / 'journal'))
    monkeypatch.delenv('TM_WRITER_LOCK_SHARED_FILE_ID', raising=False)

ATTENDANCE_FIXTURES = Path('tests/fixtures/transfermarkt/attendance')
CLUB_VALUE_FIXTURES = Path('tests/fixtures/transfermarkt/club_values')
ATTENDANCE_START = 'https://www.transfermarkt.com/premier-league/besucherzahlen/wettbewerb/GB1/saison_id/2025'
ATTENDANCE_PAGE_2 = ATTENDANCE_START + '?page=2'
CLUB_VALUE_URL = 'https://www.transfermarkt.com/v-league-1/marktwerteverein/wettbewerb/VIE1'
PAGE_1_CAPTURED = '2026-11-20T12:00:00.000000+00:00'
PAGE_2_CAPTURED = '2026-11-20T12:05:00.000000+00:00'
CLUB_CAPTURED = '2026-11-20T10:00:00.000000+00:00'
EXTRACTED_AT = '2026-11-20T12:10:00.000000+00:00'
UNIVERSE = 'core-universe-v1'


def _attendance_capture(name: str, url: str, captured_at: str) -> SourceCapture:
    body = (ATTENDANCE_FIXTURES / name).read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    return SourceCapture(
        url=url, endpoint='attendance', body=body, capture_id=digest, body_hash=digest,
        captured_at=captured_at,
    )


def _club_value_capture(url: str = CLUB_VALUE_URL) -> SourceCapture:
    body = (CLUB_VALUE_FIXTURES / 'vie1-current.html').read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    return SourceCapture(
        url=url, endpoint='club_values', body=body, capture_id=digest, body_hash=digest,
        captured_at=CLUB_CAPTURED,
    )


def _attendance_scope() -> ScopeContext:
    return ScopeContext(
        league='GB1', season='2526', scope_id='tm-scope-t10', cycle_id='tm-cycle-t10',
        edition_id='2025',
    )


def _club_value_scope() -> ScopeContext:
    # league carries the competition id in this repo's convention.
    return ScopeContext(
        league='VIE1', season='2526', scope_id='tm-scope-t11', cycle_id='tm-cycle-t11',
    )


def test_core_registries_cover_the_section_entities():
    assert 'attendance_observations' in core.CORE_ENTITIES
    assert 'club_value_observations' in core.CORE_ENTITIES
    assert core.CORE_NATURAL_KEYS['attendance_observations'] == (
        'competition_id', 'edition_id', 'club_id', 'observed_at')
    assert core.CORE_NATURAL_KEYS['club_value_observations'] == (
        'club_id', 'value_date', 'observed_at', 'source_point_id')
    assert core.CORE_WRITE_MODES['attendance_observations'] == 'replace'
    assert core.CORE_WRITE_MODES['club_value_observations'] == 'merge'
    for entity in ('attendance_observations', 'club_value_observations'):
        columns = core.CORE_COLUMNS[entity]
        for lineage in ('source_endpoint', 'raw_capture_id', 'cycle_id', 'run_id'):
            assert lineage in columns
        ddl = core.core_ddl(entity)
        assert ddl.startswith(f'CREATE TABLE IF NOT EXISTS iceberg.bronze.transfermarkt_{entity}')


def test_attendance_lane_maps_recorded_pages_to_observation_rows():
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    outcome = sections.extract_attendance_observations(
        (page_1, page_2), _attendance_scope(), start_url=ATTENDANCE_START,
        competition_id='GB1', edition_id='2025', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE,
    )
    assert outcome.entity == 'attendance_observations'
    assert outcome.status == 'observed_present'
    assert outcome.status_reason == 'attendance:ok'
    assert len(outcome.rows) == 2
    by_club = {row['club_id']: row for row in outcome.rows}
    arsenal, chelsea = by_club['11'], by_club['631']
    # Each page's rows keep that page's capture and observation clock.
    assert arsenal['source_capture_id'] == page_1.capture_id
    assert arsenal['captured_at'] == PAGE_1_CAPTURED
    assert arsenal['observed_at'] == PAGE_1_CAPTURED
    assert chelsea['source_capture_id'] == page_2.capture_id
    assert chelsea['observed_at'] == PAGE_2_CAPTURED
    assert arsenal['club_name'] == 'Arsenal FC'
    assert arsenal['capacity'] == 60704
    assert arsenal['spectators_total'] == 1151376
    assert arsenal['parser_revision'] == 'attendance-v1'
    assert chelsea['parser_revision'] == 'attendance-v1'
    assert all(row['source_url'] in (ATTENDANCE_START, ATTENDANCE_PAGE_2)
               for row in outcome.rows)
    assert all(row['observation_status'] == 'observed_present' for row in outcome.rows)
    assert outcome.capture.capture_id == page_2.capture_id  # newest page represents the unit
    dq = core.build_core_dq([outcome])
    assert dq['passed'] is True
    assert dq['row_counts']['attendance_observations'] == 2


def test_attendance_lane_reports_unproven_empty_as_unknown_without_rows():
    empty = _attendance_capture('empty.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    outcome = sections.extract_attendance_observations(
        (empty,), _attendance_scope(), start_url=ATTENDANCE_START,
        competition_id='GB1', edition_id='2025', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE,
    )
    assert outcome.status == 'unknown'
    assert outcome.status_reason == 'attendance:empty_layout_unproven'
    assert outcome.rows == ()
    assert outcome.capture.capture_id == empty.capture_id


def test_attendance_lane_maps_parse_failure_to_error():
    broken = SourceCapture(
        url=ATTENDANCE_START, endpoint='attendance', body=b'<html>not a listing</html>',
        capture_id=hashlib.sha256(b'broken').hexdigest(),
        body_hash=hashlib.sha256(b'<html>not a listing</html>').hexdigest(),
        captured_at=PAGE_1_CAPTURED,
    )
    outcome = sections.extract_attendance_observations(
        (broken,), _attendance_scope(), start_url=ATTENDANCE_START,
        competition_id='GB1', edition_id='2025', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE,
    )
    assert outcome.status == 'error'
    assert outcome.status_reason.startswith('attendance:')
    assert outcome.rows == ()
    assert outcome.capture is not None


def test_club_value_lane_splits_one_outcome_per_club():
    capture = _club_value_capture()
    outcomes = sections.extract_club_value_observations(
        capture, _club_value_scope(), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE,
    )
    assert outcomes  # tuple of per-club outcomes
    club_ids = sorted(outcome.rows[0]['club_id'] for outcome in outcomes)
    assert club_ids == [
        '116338', '13950', '14157', '20830', '20903', '24971', '32018', '35327',
        '57443', '6758', '77974', '81455', '81614', '93137']
    assert all(outcome.entity == 'club_value_observations' for outcome in outcomes)
    assert all(outcome.status == 'observed_present' for outcome in outcomes)
    for outcome in outcomes:
        clubs = {row['club_id'] for row in outcome.rows}
        assert clubs == {outcome.rows[0]['club_id']}
        assert all(row['competition_id'] == 'VIE1' for row in outcome.rows)
        assert all(row['parser_revision'] == 'club-values-v1' for row in outcome.rows)
        assert all(row['raw_capture_id'] == capture.capture_id for row in outcome.rows)
        assert all(row['cycle_id'] == 'tm-cycle-t11' for row in outcome.rows)
    rows = [row for outcome in outcomes for row in outcome.rows]
    assert len(rows) == 32
    keys = {tuple(row[column] for column in core.CORE_NATURAL_KEYS['club_value_observations'])
            for row in rows}
    assert len(keys) == 32
    dq = core.build_core_dq(outcomes)
    assert dq['passed'] is True


def test_club_value_lane_maps_parse_error_to_error_outcome():
    capture = _club_value_capture(url=CLUB_VALUE_URL.replace('/VIE1', '/ES1'))
    outcomes = sections.extract_club_value_observations(
        capture, _club_value_scope(), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE,
    )
    assert len(outcomes) == 1
    assert outcomes[0].status == 'error'
    assert outcomes[0].status_reason == 'club_values:source_url_competition_mismatch'
    assert outcomes[0].rows == ()


def test_lane_outcome_row_status_policy_for_section_entities():
    context = dict(
        capture=_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),
        scope=_attendance_scope(), extracted_at=EXTRACTED_AT, run_id='tm-core-x',
        universe_version=UNIVERSE,
    )
    # unknown outcome may carry the donor's unresolved rows
    accepted = LaneOutcome('attendance_observations', 'unknown', 'attendance:rows_unresolved',
                           ({'observation_status': 'unknown'},), **context)
    assert accepted.status == 'unknown'
    # mixed per-row statuses stay valid under an observed_present outcome
    mixed = LaneOutcome('attendance_observations', 'observed_present', 'attendance:ok',
                        ({'observation_status': 'observed_present'},
                         {'observation_status': 'explicit_none'}), **context)
    assert len(mixed.rows) == 2
    # a row status outside the section policy is still rejected
    with pytest.raises(CoreParseError):
        LaneOutcome('attendance_observations', 'observed_present', 'attendance:ok',
                    ({'observation_status': 'observed_empty'},), **context)
    # the pre-existing core entities keep the strict outcome == row rule
    squad_row = {'observation_status': 'unknown'}
    with pytest.raises(CoreParseError):
        LaneOutcome('squad_memberships', 'observed_present', 'squad_row', (squad_row,),
                    **context)


def test_native_plan_allows_multi_capture_replace_but_not_merge():
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    outcome = sections.extract_attendance_observations(
        (page_1, page_2), _attendance_scope(), start_url=ATTENDANCE_START,
        competition_id='GB1', edition_id='2025', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE,
    )
    plan = core.plan_for_outcome(
        outcome, captured_at=PAGE_2_CAPTURED,
        scope_values={'competition_id': 'GB1', 'edition_id': '2025'})
    assert isinstance(plan, NativeWritePlan)
    assert plan.write_mode == 'replace'
    assert plan.capture_id == page_2.capture_id  # newest page represents the snapshot
    assert {row['source_capture_id'] for row in plan.rows} == {page_1.capture_id, page_2.capture_id}

    # A merge plan still owns exactly one capture per plan.
    merge_rows = tuple(
        {**row, 'source_capture_id': page_1.capture_id, 'raw_capture_id': page_1.capture_id}
        for row in plan.rows)
    with pytest.raises(ValueError):
        NativeWritePlan(
            entity='club_value_observations', table='transfermarkt_club_value_observations',
            ddl=core.core_ddl('club_value_observations'),
            natural_keys=core.CORE_NATURAL_KEYS['club_value_observations'],
            scope_columns=core.CORE_SCOPE_COLUMNS['club_value_observations'],
            scope_values=('116338',), rows=merge_rows, captured_at=PAGE_1_CAPTURED,
            write_mode='merge', status='observed_present', status_reason='club_values:point',
            capture_id=page_1.capture_id,
        )


class _SectionFetch:
    """Serve section fixture bodies by exact URL (HTML pages, no raw store)."""

    def __init__(self, pages: dict[str, bytes], fail_after: int | None = None):
        self.pages = pages
        self.fail_after = fail_after
        self.calls = []

    def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
        self.calls.append(url)
        if self.fail_after is not None and len(self.calls) > self.fail_after:
            return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error='waf challenge')
        body = self.pages[url]
        digest = hashlib.sha256(body).hexdigest()
        fetched_at = PAGE_2_CAPTURED if url.endswith('?page=2') else PAGE_1_CAPTURED
        return FetchOutcome(
            status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
            raw_body_hash=digest, raw_fetched_at=fetched_at, cache_hit=False,
            status_code=200,
        )


def _attendance_pages() -> dict[str, bytes]:
    return {
        ATTENDANCE_START: (ATTENDANCE_FIXTURES / 'page_1.html').read_bytes(),
        ATTENDANCE_PAGE_2: (ATTENDANCE_FIXTURES / 'page_2.html').read_bytes(),
    }


def test_attendance_scope_run_writes_the_edition_snapshot():
    fetcher = _SectionFetch(_attendance_pages())
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t10', cycle_id='tm-cycle-t10',
        edition_id='2025',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),),
    )
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert [item['entity'] for item in report.outcomes] == ['attendance_observations']
    assert report.outcomes[0]['status'] == 'observed_present'
    assert report.outcomes[0]['rows'] == 2
    assert report.fetched == 2
    assert report.dq['passed'] is True
    assert len(report.receipts) == 1 and report.receipts[0]['verified'] is True
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_attendance_observations').fetchone()[0] == 2
    # Idempotent replay keeps the snapshot stable.
    again = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert again.receipts == report.receipts
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_attendance_observations').fetchone()[0] == 2


def test_club_value_scope_run_writes_one_bundle_per_capture_split_by_club():
    fetcher = _SectionFetch({CLUB_VALUE_URL: (CLUB_VALUE_FIXTURES / 'vie1-current.html').read_bytes()})
    plan = CoreScopePlan(
        league='VIE1', season='2526', scope_id='tm-scope-t11', cycle_id='tm-cycle-t11',
        club_value_targets=(ClubValueTarget('VIE1', 'v-league-1'),),
    )
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert {item['entity'] for item in report.outcomes} == {'club_value_observations'}
    assert all(item['status'] == 'observed_present' for item in report.outcomes)
    assert len(report.outcomes) == 14
    assert sum(item['rows'] for item in report.outcomes) == 32
    assert report.fetched == 1
    assert report.dq['passed'] is True
    assert len(report.receipts) == 1 and report.receipts[0]['verified'] is True
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_club_value_observations').fetchone()[0] == 32


def test_attendance_pagination_failure_is_unavailable_without_partial_rows():
    fetcher = _SectionFetch(_attendance_pages(), fail_after=1)
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t10', cycle_id='tm-cycle-t10',
        edition_id='2025',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),),
    )
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    report = run.run(plan)
    assert [item['status'] for item in report.outcomes] == ['unavailable']
    assert report.outcomes[0]['rows'] == 0
    assert report.receipts == ()
    assert report.dq['passed'] is True


def test_planned_urls_cover_the_section_targets():
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),),
        club_value_targets=(ClubValueTarget('VIE1', 'v-league-1', '2024-06-01'),),
    )
    assert planned_urls(plan) == (
        attendance_url('premier-league', 'GB1', 2025),
        club_values_url('v-league-1', 'VIE1', '2024-06-01'),
    )
    assert attendance_url('premier-league', 'GB1', 2025) == ATTENDANCE_START
    assert club_values_url('v-league-1', 'VIE1', '2024-06-01') == \
        CLUB_VALUE_URL + '/stichtag/2024-06-01'


# --------------------------------------------------------------------------- T8
ACHIEVEMENTS_URL = 'https://tmapi.transfermarkt.technology/player/7/achievement'
ACHIEVEMENTS_PACKET = {
    'success': True, 'message': 'ok',
    'data': {
        'aggregated': [{'id': 17, 'name': 'Copa É', 'count': 2}],
        'achievements': [
            {
                'id': '0017', 'trophyId': 31, 'seasonId': '02022',
                'season': {'id': 2022, 'name': ' 2022 '},
                'club': {'name': ' Example   XI ', 'id': 61},
                'competition': {'id': 'Q1'}, 'result': ' Runner-up ',
                'details': {'playerId': '007', 'clubId': 61, 'competitionId': 'Q1'},
            },
            {
                'id': 17, 'name': 'Detail title', 'seasonId': 2025,
                'season': {'id': 2025, 'name': '25/26'}, 'trophyId': 0,
                'club': {'id': '', 'name': 'National XI'}, 'competition': {'id': ''},
                'details': {'playerId': 0, 'competitionId': 0, 'clubId': 0},
                'sourceRowId': '0041',
            },
        ],
    },
}
ACHIEVEMENTS_EMPTY = {
    'success': True, 'message': 'ok', 'data': {'aggregated': [], 'achievements': []},
}


def _achievement_capture(payload: dict) -> SourceCapture:
    body = json.dumps(payload).encode('utf-8')
    digest = hashlib.sha256(body).hexdigest()
    return SourceCapture(
        url=ACHIEVEMENTS_URL, endpoint='player_achievement', body=body,
        capture_id=digest, body_hash=digest, captured_at=CLUB_CAPTURED,
    )


def _achievement_scope() -> ScopeContext:
    return ScopeContext(
        league='GB1', season='2526', scope_id='tm-scope-t8', cycle_id='tm-cycle-t8',
    )


def test_achievement_lane_maps_tmapi_packet_rows():
    outcome = sections.extract_achievement_observations(
        _achievement_capture(ACHIEVEMENTS_PACKET), _achievement_scope(),
        recipient_type='player', recipient_id='7', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE,
    )
    assert outcome.entity == 'achievement_observations'
    assert outcome.status == 'observed_present'
    assert outcome.status_reason == 'achievements:row'
    assert len(outcome.rows) == 2
    first, second = outcome.rows
    assert (first['title'], first['club_name'], first['result']) == (
        'Copa É', 'Example XI', 'Runner-up')
    assert first['recipient_id'] == '7'
    assert first['parser_revision'] == 'achievements-v1'
    assert first['source_capture_id'] == outcome.capture.capture_id
    assert second['source_row_id'] == 'source:41'
    keys = [tuple(row[column] for column in core.CORE_NATURAL_KEYS['achievement_observations'])
            for row in outcome.rows]
    assert len(set(keys)) == 2
    assert core.build_core_dq([outcome])['passed'] is True


def test_achievement_lane_marks_empty_collection_with_proof():
    outcome = sections.extract_achievement_observations(
        _achievement_capture(ACHIEVEMENTS_EMPTY), _achievement_scope(),
        recipient_type='player', recipient_id='7', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE,
    )
    assert outcome.status == 'observed_empty'
    assert outcome.proof == 'achievements:empty_collection:player'
    assert outcome.rows == ()


def test_achievement_lane_maps_broken_packet_to_error():
    broken = SourceCapture(
        url=ACHIEVEMENTS_URL, endpoint='player_achievement', body=b'{"success": false}',
        capture_id=hashlib.sha256(b'{"success": false}').hexdigest(),
        body_hash=hashlib.sha256(b'{"success": false}').hexdigest(),
        captured_at=CLUB_CAPTURED,
    )
    outcome = sections.extract_achievement_observations(
        broken, _achievement_scope(), recipient_type='player', recipient_id='7',
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE,
    )
    assert outcome.status == 'error'
    assert outcome.status_reason.startswith('achievements:')


class _JsonFetch:
    """Serve one JSON fixture through the client fetch contract."""

    def __init__(self, url, payload, status_code=200):
        self.url, self.payload, self.status_code = url, payload, status_code

    def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
        assert url == self.url
        body = json.dumps(self.payload).encode('utf-8')
        return FetchOutcome(
            status=FetchStatus.OK, value=self.payload,
            raw_capture_id=hashlib.sha256(body).hexdigest(),
            raw_body_hash=hashlib.sha256(body).hexdigest(),
            raw_fetched_at=CLUB_CAPTURED, cache_hit=False, status_code=self.status_code,
        )

    def raw_loader(self, capture_id):
        body = json.dumps(self.payload).encode('utf-8')
        assert capture_id == hashlib.sha256(body).hexdigest()
        return body, _JsonRecord(self.url, hashlib.sha256(body).hexdigest(), CLUB_CAPTURED)


class _JsonRecord:
    def __init__(self, url, content_hash, fetched_at):
        self.url, self.content_hash, self.fetched_at = url, content_hash, fetched_at


def test_achievement_holder_scope_run_writes_one_merge_bundle():
    from scrapers.transfermarkt.orchestrator import AchievementHolderTarget, achievements_url
    assert achievements_url('player', '7') == ACHIEVEMENTS_URL
    replay = _JsonFetch(ACHIEVEMENTS_URL, ACHIEVEMENTS_PACKET)
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t8', cycle_id='tm-cycle-t8',
        achievement_holders=(AchievementHolderTarget('player', '7'),),
    )
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader,
                  universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert [item['entity'] for item in report.outcomes] == ['achievement_observations']
    assert report.outcomes[0]['status'] == 'observed_present'
    assert report.outcomes[0]['rows'] == 2
    assert report.dq['passed'] is True and report.dq['verified_write_coverage'] == 1.0
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_achievement_observations').fetchone()[0] == 2


def test_achievement_json_capture_fails_closed_without_raw_loader():
    from scrapers.transfermarkt.orchestrator import AchievementHolderTarget
    replay = _JsonFetch(ACHIEVEMENTS_URL, ACHIEVEMENTS_PACKET)
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t8', cycle_id='tm-cycle-t8',
        achievement_holders=(AchievementHolderTarget('player', '7'),),
    )
    run = CoreRun(replay.fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    with pytest.raises(CoreRunError, match='raw body loader'):
        run.run(plan)


# --------------------------------------------------------------------------- T13
from scrapers.transfermarkt.rumours import CompetitionContext
from tests.unit.scrapers.test_transfermarkt_rumours import _GRID_TABLE

RUMOUR_LISTING = 'https://www.transfermarkt.com/-/geruechte/wettbewerb/GB1'
RUMOUR_PAGE = {number: f'{RUMOUR_LISTING}/page/{number}' for number in (1, 2)}
RUMOUR_PAGE_1_CAPTURED = '2026-11-20T21:21:09.485303+00:00'
RUMOUR_PAGE_2_CAPTURED = '2026-11-20T21:21:24.425970+00:00'
# The donor requires extracted_at >= captured_at; the saved GB1 clocks are late evening.
RUMOUR_EXTRACTED_AT = '2026-11-20T21:30:00.000000+00:00'


def _rumour_pager(active: int, last: int) -> str:
    items = []
    for number in range(1, last + 1):
        cls = ' tm-pagination__list-item--active' if number == active else ''
        items.append(
            f'<li class="tm-pagination__list-item{cls}">'
            f'<a class="tm-pagination__link" href="{RUMOUR_LISTING}/page/{number}">{number}</a></li>')
    if active != last:
        # The donor infers the last page only from a control or a complete tail.
        items.append(
            f'<li class="tm-pagination__list-item--icon-last-page">'
            f'<a class="tm-pagination__link" href="{RUMOUR_LISTING}/page/{last}"></a></li>')
    return f'<div class="pager"><ul>{"".join(items)}</ul></div>'


def _rumour_page_html(active: int) -> bytes:
    return f'<div id="yw1">{_GRID_TABLE}{_rumour_pager(active, 2)}</div>'.encode('utf-8')


def _rumour_capture(number: int, captured_at: str, body: bytes | None = None,
                    url: str | None = None) -> SourceCapture:
    body = body if body is not None else _rumour_page_html(number)
    digest = hashlib.sha256(body).hexdigest()
    return SourceCapture(
        url=url or RUMOUR_PAGE[number], endpoint='rumours', body=body, capture_id=digest,
        body_hash=digest, captured_at=captured_at,
    )


def _rumour_competition() -> CompetitionContext:
    return CompetitionContext('GB1', 'Premier League',
                              'https://www.transfermarkt.com/-/startseite/wettbewerb/GB1')


def _rumour_scope() -> ScopeContext:
    return ScopeContext(
        league='GB1', season='2526', scope_id='tm-scope-t13', cycle_id='tm-cycle-t13',
    )


def test_rumour_lane_maps_complete_feed_to_observation_rows():
    outcomes = sections.extract_rumour_observations(
        (_rumour_capture(1, RUMOUR_PAGE_1_CAPTURED), _rumour_capture(2, RUMOUR_PAGE_2_CAPTURED)),
        _rumour_scope(), competition=_rumour_competition(), extracted_at=RUMOUR_EXTRACTED_AT,
        universe_version=UNIVERSE,
    )
    # One write-plan outcome per page: a merge plan owns exactly one capture.
    assert len(outcomes) == 2
    assert all(outcome.entity == 'rumour_observations' for outcome in outcomes)
    assert all(outcome.status == 'observed_present' for outcome in outcomes)
    rows = [row for outcome in outcomes for row in outcome.rows]
    by_page = {row['page_number']: row for row in rows}
    assert set(by_page) == {1, 2}
    first = by_page[1]
    assert first['player_name'] == 'Player P'
    assert first['interested_club_name'] == 'Club C'
    assert first['parser_revision'] == 'transfermarkt-rumours/2'
    assert first['source_capture_id'] == hashlib.sha256(_rumour_page_html(1)).hexdigest()
    assert by_page[2]['source_capture_id'] == hashlib.sha256(_rumour_page_html(2)).hexdigest()
    assert outcomes[0].capture.capture_id == hashlib.sha256(_rumour_page_html(1)).hexdigest()
    assert outcomes[1].capture.capture_id == hashlib.sha256(_rumour_page_html(2)).hexdigest()
    keys = {tuple(row[column] for column in core.CORE_NATURAL_KEYS['rumour_observations'])
            for row in rows}
    assert len(keys) == 2  # the same thread observed at two page clocks
    assert core.build_core_dq(outcomes)['passed'] is True


def test_rumour_lane_reports_incomplete_saved_feed_as_error():
    saved = Path('tests/fixtures/transfermarkt/rumours')
    page_1 = SourceCapture(
        url=RUMOUR_PAGE[1], endpoint='rumours', body=(saved / 'gb1-page-1.html').read_bytes(),
        capture_id='53ae52cdcb469843f3de7d23a9722e3a4b5578e71a59d7cb0f5b6ee107ba3edc',
        body_hash='53ae52cdcb469843f3de7d23a9722e3a4b5578e71a59d7cb0f5b6ee107ba3edc',
        captured_at=RUMOUR_PAGE_1_CAPTURED)
    page_2 = SourceCapture(
        url=RUMOUR_PAGE[2], endpoint='rumours', body=(saved / 'gb1-page-2.html').read_bytes(),
        capture_id='33e9f518feea9bccd4463cdfa3d7437a2ad398695381caad4e4b56e700a88687',
        body_hash='33e9f518feea9bccd4463cdfa3d7437a2ad398695381caad4e4b56e700a88687',
        captured_at=RUMOUR_PAGE_2_CAPTURED)
    outcome, = sections.extract_rumour_observations(
        (page_1, page_2), _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE,
    )
    # The saved sample stops at page 2 of a 5-page plan: no complete feed, no rows.
    assert outcome.status == 'error'
    assert outcome.status_reason.startswith('rumours:')
    assert outcome.rows == ()


def test_rumour_lane_maps_confirmed_empty_grid():
    empty = SourceCapture(
        url=RUMOUR_LISTING, endpoint='rumours',
        body=b'<div id="yw1"><span class="empty">No information available.</span>'
             b'<div class="keys"></div></div>',
        capture_id=hashlib.sha256(b'empty-grid').hexdigest(),
        body_hash=hashlib.sha256(
            b'<div id="yw1"><span class="empty">No information available.</span>'
            b'<div class="keys"></div></div>').hexdigest(),
        captured_at=RUMOUR_PAGE_1_CAPTURED)
    outcome, = sections.extract_rumour_observations(
        (empty,), _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE,
    )
    assert outcome.status == 'observed_empty'
    assert outcome.proof == 'rumours:confirmed_empty_grid'
    assert outcome.rows == ()


def test_rumour_scope_run_walks_the_page_plan_and_writes():
    from scrapers.transfermarkt.orchestrator import RumourCompetitionTarget, rumours_url
    assert rumours_url('-', 'GB1') == RUMOUR_LISTING
    pages = {
        RUMOUR_LISTING: (_rumour_page_html(1), RUMOUR_PAGE_1_CAPTURED),
        RUMOUR_PAGE[1]: (_rumour_page_html(1), RUMOUR_PAGE_1_CAPTURED),
        RUMOUR_PAGE[2]: (_rumour_page_html(2), RUMOUR_PAGE_2_CAPTURED),
    }

    class _Fetch:
        def __init__(self):
            self.calls = []

        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            self.calls.append(url)
            body, captured_at = pages[url]
            digest = hashlib.sha256(body).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
                raw_body_hash=digest, raw_fetched_at=captured_at, cache_hit=False,
                status_code=200)

    fetcher = _Fetch()
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t13', cycle_id='tm-cycle-t13',
        rumour_competitions=(RumourCompetitionTarget('GB1', 'Premier League', '-'),),
    )
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=RUMOUR_EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    # Discovery at the listing plus every canonical plan page (page 1 included:
    # the donor binds each row to the canonical /page/N URL).
    assert fetcher.calls == [RUMOUR_LISTING, RUMOUR_PAGE[1], RUMOUR_PAGE[2]]
    assert [item['entity'] for item in report.outcomes] == ['rumour_observations'] * 2
    assert all(item['status'] == 'observed_present' for item in report.outcomes)
    assert sum(item['rows'] for item in report.outcomes) == 2
    assert report.dq['passed'] is True
    assert len(report.receipts) == 1 and report.receipts[0]['verified'] is True
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_rumour_observations').fetchone()[0] == 2


def test_rumour_pagination_failure_is_unavailable_without_partial_rows():
    from scrapers.transfermarkt.orchestrator import RumourCompetitionTarget

    class _Fetch:
        def __init__(self):
            self.calls = []

        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            self.calls.append(url)
            if url == RUMOUR_PAGE[2]:
                return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error='waf challenge')
            body, captured_at = _rumour_page_html(1), RUMOUR_PAGE_1_CAPTURED
            digest = hashlib.sha256(body).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
                raw_body_hash=digest, raw_fetched_at=captured_at, cache_hit=False,
                status_code=200)

    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t13', cycle_id='tm-cycle-t13',
        rumour_competitions=(RumourCompetitionTarget('GB1', 'Premier League', '-'),),
    )
    run = CoreRun(_Fetch().fetch, universe_version=UNIVERSE, extracted_at=RUMOUR_EXTRACTED_AT)
    report = run.run(plan)
    assert [item['status'] for item in report.outcomes] == ['unavailable']
    assert report.receipts == ()
    assert report.dq['passed'] is True


# --------------------------------------------------------------------------- T12
REFEREE_FIXTURES = Path('tests/fixtures/transfermarkt/referees')
REFEREE_DIRECTORY_URL = (
    'https://www.transfermarkt.com/example-league/schiedsrichter/wettbewerb/XY1'
    '/saison_id/2025/plus/1')
REFEREE_PROFILE_URL = 'https://www.transfermarkt.com/-/profil/schiedsrichter/101'
REFEREE_CAPTURED = '2026-11-20T15:00:00.000000+00:00'
REFEREE_EXTRACTED_AT = '2026-11-20T15:10:00.000000+00:00'


def _referee_directory_capture(captured_at: str = REFEREE_CAPTURED,
                               name: str = 'directory_compact.html') -> SourceCapture:
    body = (REFEREE_FIXTURES / name).read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    return SourceCapture(
        url=REFEREE_DIRECTORY_URL, endpoint='referee_directory', body=body,
        capture_id=digest, body_hash=digest, captured_at=captured_at,
    )


def _referee_profile_capture() -> SourceCapture:
    body = (REFEREE_FIXTURES / 'profile.html').read_bytes()
    digest = hashlib.sha256(body).hexdigest()
    return SourceCapture(
        url=REFEREE_PROFILE_URL, endpoint='referee_profile', body=body,
        capture_id=digest, body_hash=digest, captured_at=REFEREE_CAPTURED,
    )


def _referee_scope() -> ScopeContext:
    return ScopeContext(
        league='XY1', season='2526', scope_id='tm-scope-t12', cycle_id='tm-cycle-t12',
    )


def test_referee_directory_lane_maps_aggregates_per_page():
    outcomes = sections.extract_referee_directory_observations(
        (_referee_directory_capture(),), _referee_scope(),
        extracted_at=REFEREE_EXTRACTED_AT, universe_version=UNIVERSE,
    )
    outcome, = outcomes
    assert outcome.entity == 'referee_aggregate_observations'
    assert outcome.status == 'observed_present'
    assert len(outcome.rows) == 10
    by_referee = {}
    for row in outcome.rows:
        by_referee.setdefault(row['referee_id'], []).append(row)
    assert sorted(by_referee) == ['101', '202']
    sample = by_referee['101'][0]
    assert sample['competition_id'] == 'XY1' and sample['edition_id'] == '2025'
    assert sample['observation_status'] == 'observed_present'
    assert sample['source_capture_id'] == outcome.capture.capture_id
    assert core.build_core_dq(outcomes)['passed'] is True
    values = {row['metric']: row['value'] for row in by_referee['101']}
    assert values['matches_officiated'] == '14'
    assert values['penalties_awarded'] == '4'


def test_referee_profile_lane_maps_identity_fields():
    outcome = sections.extract_referee_profile_observations(
        _referee_profile_capture(), _referee_scope(), referee_id='101',
        extracted_at=REFEREE_EXTRACTED_AT, universe_version=UNIVERSE,
    )
    assert outcome.entity == 'referee_profiles'
    assert outcome.status == 'observed_present'
    row, = outcome.rows
    assert (row['referee_id'], row['name']) == ('101', 'Ada Example')
    assert row['nationality'] == 'Imaginaria'
    assert row['birth_date'] == '1990-03-10'
    assert row['observed_at'] == REFEREE_CAPTURED


def test_referee_directory_scope_run_writes_one_bundle_per_page():
    from scrapers.transfermarkt.orchestrator import RefereeDirectoryTarget

    class _Fetch:
        def __init__(self):
            self.calls = []

        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            self.calls.append(url)
            body = (REFEREE_FIXTURES / 'directory_compact.html').read_bytes()
            digest = hashlib.sha256(body).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
                raw_body_hash=digest, raw_fetched_at=REFEREE_CAPTURED, cache_hit=False,
                status_code=200)

    fetcher = _Fetch()
    plan = CoreScopePlan(
        league='XY1', season='2526', scope_id='tm-scope-t12', cycle_id='tm-cycle-t12',
        referee_directories=(RefereeDirectoryTarget('XY1', '2025', 'example-league'),),
    )
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=REFEREE_EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert fetcher.calls == [REFEREE_DIRECTORY_URL]
    assert [item['entity'] for item in report.outcomes] == ['referee_aggregate_observations']
    assert report.outcomes[0]['rows'] == 10
    assert report.dq['passed'] is True
    assert len(report.receipts) == 1 and report.receipts[0]['verified'] is True
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_referee_aggregate_observations').fetchone()[0] == 10


def test_referee_profile_scope_run_writes_the_profile_row():
    class _Fetch:
        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            assert url == REFEREE_PROFILE_URL
            body = (REFEREE_FIXTURES / 'profile.html').read_bytes()
            digest = hashlib.sha256(body).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
                raw_body_hash=digest, raw_fetched_at=REFEREE_CAPTURED, cache_hit=False,
                status_code=200)

    plan = CoreScopePlan(
        league='XY1', season='2526', scope_id='tm-scope-t12', cycle_id='tm-cycle-t12',
        referee_profile_ids=('101',),
    )
    run = CoreRun(_Fetch().fetch, universe_version=UNIVERSE, extracted_at=REFEREE_EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert [item['entity'] for item in report.outcomes] == ['referee_profiles']
    assert report.outcomes[0]['rows'] == 1
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_referee_profiles').fetchone()[0] == 1


# ---------------------------------------------------------------------------- T2
REPRESENTATION_URL = (
    'https://tmapi.transfermarkt.technology/players?ids[]=7&ids[]=9')
REPRESENTATION_PACKET = {
    'success': True,
    'data': [
        {
            'id': '7',
            'attributes': {'consultantAgencyId': 55},
            'clubAssignments': [{'clubId': 11, 'type': 'current'}],
        },
        {
            'id': '9',
            'attributes': {},
            'clubAssignments': [{'clubId': 631, 'type': 'current'}],
        },
    ],
}


def _representation_capture(payload: dict | None = None) -> SourceCapture:
    body = json.dumps(payload or REPRESENTATION_PACKET).encode('utf-8')
    digest = hashlib.sha256(body).hexdigest()
    return SourceCapture(
        url=REPRESENTATION_URL, endpoint='representation', body=body,
        capture_id=digest, body_hash=digest, captured_at=CLUB_CAPTURED,
    )


def _representation_scope() -> ScopeContext:
    return ScopeContext(
        league='GB1', season='2526', scope_id='tm-scope-t2', cycle_id='tm-cycle-t2',
    )


def test_representation_lane_maps_packet_assertions_per_player():
    outcomes = sections.extract_representation_observations(
        _representation_capture(), _representation_scope(), expected_ids=('7', '9'),
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE,
    )
    assert [outcome.rows[0]['player_id'] if outcome.rows else outcome.scope.player_id
            for outcome in outcomes] == ['7', '9']
    assigned, unknown = outcomes
    assert assigned.entity == 'player_representation_observations'
    assert assigned.status == 'observed_present'
    row, = assigned.rows
    assert row['relation_type'] == 'consultant_agency'
    assert row['representative_key'] == 'agency:55'
    assert row['representative_id'] == '55'
    assert row['observation_status'] == 'observed_present'
    assert unknown.status == 'unknown'
    assert unknown.rows[0]['representative_key'] == 'status:unknown'
    assert unknown.rows[0]['representative_id'] is None
    assert core.build_core_dq(outcomes)['passed'] is True


def test_representation_scope_run_batches_players_into_one_packet():
    from scrapers.transfermarkt.orchestrator import representation_url
    assert representation_url(('7', '9')) == REPRESENTATION_URL
    replay = _JsonFetch(REPRESENTATION_URL, REPRESENTATION_PACKET)
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t2', cycle_id='tm-cycle-t2',
        representation_player_ids=('7', '9'),
    )
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader,
                  universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert report.fetched == 1
    assert {item['status'] for item in report.outcomes} == {'observed_present', 'unknown'}
    assert report.dq['passed'] is True
    # Only the conclusive (assigned) player writes; the unknown one waits for
    # a later conclusive capture — stage-B conclusive-only write policy.
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_player_representation_observations'
    ).fetchone()[0] == 1


# --------------------------------------------------- review r1 regression pins
def test_outcome_scope_cannot_override_the_requested_target():
    from scrapers.transfermarkt.orchestrator import _outcome_scope_values
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        coach_profile_ids=(('202', 'x'),),
    )
    request = next(r for r, _ in _iter_requests(plan) if r.label == 'coach_profile')
    outcome = core.LaneOutcome(
        'coach_profiles', 'observed_present', 'coach_profile',
        ({'coach_id': '101', 'observation_status': 'observed_present'},),
        **core._context(_coach_capture('101'), core.ScopeContext(
            league='GB1', season='2627', scope_id='s', cycle_id='c'), EXTRACTED_AT, UNIVERSE))
    with pytest.raises(CoreRunError, match='escape the requested scope'):
        _outcome_scope_values(outcome, request, plan)


def _coach_capture(coach_id: str) -> SourceCapture:
    body = b'<html></html>'
    return SourceCapture(
        url=f'https://www.transfermarkt.com/x/profil/trainer/{coach_id}',
        endpoint='coach_profile', body=body, capture_id=hashlib.sha256(body).hexdigest(),
        body_hash=hashlib.sha256(body).hexdigest(), captured_at=CLUB_CAPTURED)


def _iter_requests(plan):
    from scrapers.transfermarkt import orchestrator
    return orchestrator._requests(plan)


def test_rumour_discovery_cycle_fails_closed():
    from scrapers.transfermarkt.orchestrator import RumourCompetitionTarget

    def pager(href_for_one: str) -> str:
        return ('<div class="pager"><ul>'
                '<li class="tm-pagination__list-item tm-pagination__list-item--active">'
                f'<a class="tm-pagination__link" href="{href_for_one}">1</a></li>'
                '</ul></div>')

    class _Fetch:
        """Listing names /page/1 canonical; /page/1 names the listing back."""

        def __init__(self):
            self.calls = []

        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            self.calls.append(url)
            href = RUMOUR_PAGE[1] if url == RUMOUR_LISTING else RUMOUR_LISTING
            body = f'<div id="yw1">{_GRID_TABLE}{pager(href)}</div>'.encode()
            digest = hashlib.sha256(body).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
                raw_body_hash=digest, raw_fetched_at=RUMOUR_PAGE_1_CAPTURED, cache_hit=False,
                status_code=200)

    fetcher = _Fetch()
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t13', cycle_id='tm-cycle-t13',
        rumour_competitions=(RumourCompetitionTarget('GB1', 'Premier League', '-'),),
    )
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=RUMOUR_EXTRACTED_AT)
    with pytest.raises(CoreRunError, match='discovery_cycle'):
        run.run(plan)
    # The guard rejects the conflicting re-entry BEFORE fetching it again.
    assert fetcher.calls == [RUMOUR_LISTING, RUMOUR_PAGE[1]]


def test_achievement_adapter_treats_http_failure_as_unavailable():
    body = json.dumps(ACHIEVEMENTS_EMPTY).encode('utf-8')
    digest = hashlib.sha256(body).hexdigest()
    failed = SourceCapture(
        url=ACHIEVEMENTS_URL, endpoint='player_achievement', body=body,
        capture_id=digest, body_hash=digest, captured_at=CLUB_CAPTURED, status_code=404,
    )
    outcome = sections.extract_achievement_observations(
        failed, _achievement_scope(), recipient_type='player', recipient_id='7',
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE,
    )
    assert outcome.status == 'unavailable'
    assert outcome.status_reason == 'achievements:http_404'
    assert outcome.rows == ()


# ------------------------------------- review r1 deferred pins (r2 cycle, #2405)
STALE_CAPTURED = '2026-11-20T11:00:00.000000+00:00'
NEWER_PAGE_CAPTURED = '2026-11-20T12:30:00.000000+00:00'
LATER_EXTRACTED_AT = '2026-11-20T13:00:00.000000+00:00'


def _attendance_plan(captures, extracted_at=EXTRACTED_AT):
    outcome = sections.extract_attendance_observations(
        captures, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
        edition_id='2025', extracted_at=extracted_at, universe_version=UNIVERSE)
    # The plan clock is the outcome's capture: the newest page WITH rows.
    return core.plan_for_outcome(
        outcome, captured_at=outcome.capture.captured_at,
        scope_values={'competition_id': 'GB1', 'edition_id': '2025'})


def _bundle(db, plans, committed_at):
    return native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), plans, run_id=plans[0].rows[0]['run_id'],
        universe_version=UNIVERSE, committed_at=committed_at)


def test_stale_page_cannot_overwrite_a_fresher_committed_snapshot():
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    # An older observation of page 1 of the same edition: same URL, other body.
    older_body = (ATTENDANCE_FIXTURES / 'page_1.html').read_bytes().replace(
        b'Emirates Stadium', b'Ashburton Grove')
    older_digest = hashlib.sha256(older_body).hexdigest()
    older_page_1 = SourceCapture(
        url=ATTENDANCE_START, endpoint='attendance', body=older_body,
        capture_id=older_digest, body_hash=older_digest, captured_at=STALE_CAPTURED)
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    # The replay mixes the stale page 1 with the unchanged page 2: its newest
    # clock equals the committed one, so only a per-page check can refuse it.
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, [_attendance_plan((older_page_1, page_2), LATER_EXTRACTED_AT)],
                LATER_EXTRACTED_AT)
    assert db.sql.execute(
        'SELECT stadium_name FROM transfermarkt_attendance_observations WHERE club_id = ?',
        ('11',)).fetchone()[0] == 'Emirates Stadium'


def test_refreshed_page_reuses_its_unchanged_sibling_pages():
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    # A later capture of the same page: same body, its own capture identity.
    newer_page_1 = SourceCapture(
        url=ATTENDANCE_START, endpoint='attendance', body=page_1.body,
        capture_id=hashlib.sha256(b'newer page 1').hexdigest(), body_hash=page_1.body_hash,
        captured_at=NEWER_PAGE_CAPTURED)
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    # A refreshed snapshot may reuse the cached page 2 beside a newer page 1.
    receipt = _bundle(db, [_attendance_plan((newer_page_1, page_2), LATER_EXTRACTED_AT)],
                      LATER_EXTRACTED_AT)
    assert receipt['verified'] is True
    clocks = dict(db.sql.execute(
        'SELECT club_id, captured_at FROM transfermarkt_attendance_observations').fetchall())
    assert clocks == {'11': NEWER_PAGE_CAPTURED, '631': PAGE_2_CAPTURED}


def test_reobserved_multi_page_merge_refreshes_lineage_beside_a_newer_sibling_page():
    pages = (_rumour_capture(1, RUMOUR_PAGE_1_CAPTURED), _rumour_capture(2, RUMOUR_PAGE_2_CAPTURED))
    later = '2026-11-20T21:45:00.000000+00:00'  # after both saved page clocks

    def plans(extracted_at):
        outcomes = sections.extract_rumour_observations(
            pages, _rumour_scope(), competition=_rumour_competition(),
            extracted_at=extracted_at, universe_version=UNIVERSE)
        return [core.plan_for_outcome(
            outcome, captured_at=outcome.capture.captured_at,
            scope_values={'competition_id': 'GB1'}) for outcome in outcomes]

    db = CoreDB()
    _bundle(db, plans(RUMOUR_EXTRACTED_AT), RUMOUR_EXTRACTED_AT)
    # The same two captures in a later run: page 1 is not stale merely because
    # its sibling page 2 carries the newer clock of the same feed.
    again = _bundle(db, plans(later), later)
    assert again['verified'] is True
    later_run = plans(later)[0].rows[0]['run_id']
    rows = db.sql.execute(
        'SELECT run_id, page_number FROM transfermarkt_rumour_observations ORDER BY page_number'
    ).fetchall()
    assert rows == [(later_run, 1), (later_run, 2)]  # lineage refreshed, no duplicate rows


def _outcome_context(entity='attendance_observations'):
    return dict(
        capture=_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),
        scope=_attendance_scope(), extracted_at=EXTRACTED_AT, run_id='tm-core-x',
        universe_version=UNIVERSE)


def test_proof_statuses_carry_rows_in_no_lane():
    context = _outcome_context()
    # A proof certifies an absence; the referee lane allows observed_empty as a
    # metric verdict, which must not let a populated page hide behind a proof.
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        LaneOutcome('referee_aggregate_observations', 'observed_empty', 'referees:empty',
                    ({'observation_status': 'observed_present'},),
                    proof='referees:confirmed_empty', **context)
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        LaneOutcome('attendance_observations', 'not_applicable', 'attendance:x',
                    ({'observation_status': 'explicit_none'}), proof='p', **context)
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        LaneOutcome('player_contract_observations', 'not_applicable', 'x',
                    ({'observation_status': 'not_applicable', 'contract_status': 'not_rendered'}),
                    proof='p', **context)


@pytest.mark.parametrize('entity, status, reason, verdicts', [
    # explicit_none aggregates only explicit_none rows (donor summarise rule)
    ('attendance_observations', 'explicit_none', 'attendance:all_rows_explicit_none',
     ['observed_present']),
    ('attendance_observations', 'explicit_none', 'attendance:all_rows_explicit_none',
     ['explicit_none', 'unknown']),
    ('club_value_observations', 'explicit_none', 'club_values:club_rows_explicit_none', ['unknown']),
    ('player_representation_observations', 'explicit_none', 'representation:explicit_none',
     ['unknown']),
    # observed_present needs a present row in the presence lanes
    ('attendance_observations', 'observed_present', 'attendance:ok', ['unknown']),
    ('attendance_observations', 'observed_present', 'attendance:ok',
     ['explicit_none', 'unknown']),
    ('club_value_observations', 'observed_present', 'club_values:club_rows_present',
     ['explicit_none']),
    ('player_representation_observations', 'observed_present', 'representation:assigned',
     ['explicit_none']),
    # unknown cannot hide a present row
    ('attendance_observations', 'unknown', 'attendance:rows_unresolved',
     ['observed_present', 'unknown']),
    ('club_value_observations', 'unknown', 'club_values:club_rows_unresolved',
     ['observed_present', 'unknown']),
    ('player_representation_observations', 'unknown', 'representation:unknown',
     ['observed_present']),
])
def test_aggregate_verdict_must_agree_with_its_row_verdicts(entity, status, reason, verdicts):
    rows = tuple({'observation_status': verdict} for verdict in verdicts)
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        LaneOutcome(entity, status, reason, rows, **_outcome_context())


@pytest.mark.parametrize('entity, status, reason, verdicts', [
    ('attendance_observations', 'observed_present', 'attendance:ok',
     ['observed_present', 'explicit_none', 'unknown']),
    ('attendance_observations', 'unknown', 'attendance:rows_unresolved',
     ['unknown', 'explicit_none']),
    ('attendance_observations', 'explicit_none', 'attendance:all_rows_explicit_none',
     ['explicit_none']),
    ('club_value_observations', 'unknown', 'club_values:club_rows_unresolved', ['unknown']),
    # The referee page outcome certifies the page's entries; its rows carry
    # metric-level verdicts, including a page whose every cell is a dash.
    ('referee_aggregate_observations', 'observed_present', 'referees:page',
     ['explicit_none', 'observed_empty', 'observed_present']),
    ('referee_aggregate_observations', 'observed_present', 'referees:page', ['explicit_none']),
])
def test_consistent_aggregates_and_metric_pages_stay_valid(entity, status, reason, verdicts):
    rows = tuple({'observation_status': verdict} for verdict in verdicts)
    assert LaneOutcome(entity, status, reason, rows, **_outcome_context()).status == status


def test_populated_squad_page_keeps_its_contract_cell_exception():
    context = _outcome_context('player_contract_observations')
    rows = tuple({'observation_status': verdict, 'contract_status': contract} for verdict, contract in (
        ('observed_present', 'ok'), ('unknown', 'unknown'), ('not_applicable', 'not_rendered')))
    outcome = LaneOutcome('player_contract_observations', 'observed_present', 'squad_page',
                          rows, **context)
    assert len(outcome.rows) == 3
    # A contract cell still has to agree with its own mapped verdict.
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        LaneOutcome('player_contract_observations', 'observed_present', 'squad_page',
                    ({'observation_status': 'unknown', 'contract_status': 'ok'},), **context)
    # Core lanes keep the strict outcome == row rule.
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        LaneOutcome('rumour_observations', 'observed_present', 'rumours:page',
                    ({'observation_status': 'explicit_none'},), **context)


def test_empty_outcomes_carry_their_section_revisions_into_receipts():
    from scrapers.transfermarkt import achievements
    outcome = sections.extract_achievement_observations(
        _achievement_capture(ACHIEVEMENTS_EMPTY), _achievement_scope(),
        recipient_type='player', recipient_id='7', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE)
    plan = core.plan_for_outcome(outcome, captured_at=CLUB_CAPTURED,
                                 scope_values={'recipient_type': 'player', 'recipient_id': '7'})
    # With no row to speak for the lane, the outcome names its own revisions.
    assert dict(plan.evidence)['parser_revision'] == achievements.ACHIEVEMENT_PARSER_REVISION
    assert dict(plan.evidence)['schema_revision'] == str(achievements.ACHIEVEMENT_SCHEMA_REVISION)
    db = CoreDB()
    native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), [plan], run_id=outcome.run_id,
        universe_version=UNIVERSE, committed_at=EXTRACTED_AT)
    assert db.sql.execute(
        'SELECT parser_revision, schema_revision, row_count, observation_status '
        'FROM transfermarkt_core_receipts_v1').fetchone() == (
        'achievements-v1', '1', 0, 'observed_empty')
    assert core.build_core_dq([outcome])['passed'] is True


def test_populated_outcome_names_the_revision_of_its_rows():
    from scrapers.transfermarkt import attendance
    outcome = sections.extract_attendance_observations(
        (_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),),
        _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1', edition_id='2025',
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert outcome.parser_revision == attendance.ATTENDANCE_PARSER_REVISION
    assert outcome.schema_revision == attendance.ATTENDANCE_SCHEMA_REVISION
    # An outcome cannot claim a revision its rows do not carry.
    with pytest.raises(CoreParseError, match='revision'):
        LaneOutcome('attendance_observations', 'observed_present', 'attendance:ok',
                    ({'observation_status': 'observed_present', 'parser_revision': 'other-v9'},),
                    parser_revision=attendance.ATTENDANCE_PARSER_REVISION,
                    **_outcome_context())


def test_failed_targets_of_one_scope_stay_distinct_in_dq():
    from scrapers.transfermarkt.orchestrator import AchievementHolderTarget

    class _Blocked:
        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error='waf challenge')

    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t8', cycle_id='tm-cycle-t8',
        achievement_holders=(AchievementHolderTarget('player', '7'), AchievementHolderTarget('club', '13')),
    )
    report = CoreRun(_Blocked().fetch, universe_version=UNIVERSE,
                     extracted_at=EXTRACTED_AT).run(plan)
    # Two failed holders of one scope: neither a duplicate nor anonymous.
    assert report.dq['passed'] is True
    assert report.dq['outcome_status_counts'] == {'unavailable': 2}
    assert {item['subject'] for item in report.outcomes} == {
        '{"recipient_id":"7","recipient_type":"player"}',
        '{"recipient_id":"13","recipient_type":"club"}'}

    careers = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        market_value_player_ids=('1', '2'), transfer_player_ids=('1',))
    report = CoreRun(_Blocked().fetch, universe_version=UNIVERSE,
                     extracted_at=EXTRACTED_AT).run(careers)
    assert report.dq['passed'] is True
    assert {item['subject'] for item in report.outcomes} == {
        '{"player_id":"1"}', '{"player_id":"2"}'}


def test_failed_representation_packets_stay_distinct_in_dq():
    class _BrokenPacket:
        """Both packets of the run answer with a body that is not JSON."""
        body = b'{"success": "nope"'

        def __init__(self):
            self.urls = []

        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            self.urls.append(url)
            digest = hashlib.sha256(self.body).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, raw_capture_id=digest, raw_body_hash=digest,
                raw_fetched_at=CLUB_CAPTURED, cache_hit=False, status_code=200)

        def raw_loader(self, capture_id):
            return self.body, _JsonRecord(self.urls[-1], capture_id, CLUB_CAPTURED)

    players = tuple(str(number) for number in range(1, 102))  # packets of 100 and 1
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t2', cycle_id='tm-cycle-t2',
        representation_player_ids=players)
    replay = _BrokenPacket()
    report = CoreRun(replay.fetch, raw_loader=replay.raw_loader, universe_version=UNIVERSE,
                     extracted_at=EXTRACTED_AT).run(plan)
    assert [item['status'] for item in report.outcomes] == ['error', 'error']
    assert report.dq['passed'] is True
    assert {item['subject'] for item in report.outcomes} == {
        core.canonical_json({'player_ids': list(players[:100])}).decode(),
        core.canonical_json({'player_ids': ['101']}).decode()}


def _rumour_feed_html(active: int, last: int, numbers, *, last_control=None) -> bytes:
    """One feed page whose pager evidences exactly the listed page numbers."""
    if last_control is None:
        # The donor infers the last page only from a control or a complete tail.
        last_control = active != last
    items = []
    for number in numbers:
        active_class = ' tm-pagination__list-item--active' if number == active else ''
        items.append(
            f'<li class="tm-pagination__list-item{active_class}">'
            f'<a class="tm-pagination__link" href="{RUMOUR_LISTING}/page/{number}">{number}</a></li>')
    if last_control:
        items.append(
            f'<li class="tm-pagination__list-item--icon-last-page">'
            f'<a class="tm-pagination__link" href="{RUMOUR_LISTING}/page/{last}"></a></li>')
    return f'<div id="yw1">{_GRID_TABLE}<div class="pager"><ul>{"".join(items)}</ul></div></div>'.encode()


_NO_ENTRIES_TABLE = (
    '<table class="items"><thead><tr><th>Player</th><th>Interested club</th>'
    '<th>Most recent source from</th><th>Assessment</th></tr></thead><tbody><tr>'
    '<td colspan="4" class="no-entries">No entries available</td></tr></tbody></table>')


def _no_entries_html(active: int, last: int) -> bytes:
    """A donor-valid empty feed page: the table rendered its no-entries row."""
    return f'<div id="yw1">{_NO_ENTRIES_TABLE}{_rumour_pager(active, last)}</div>'.encode()


def _rumour_page_url(number: int) -> str:
    return f'{RUMOUR_LISTING}/page/{number}'


class _FeedFetch:
    """Serve rumour feed pages with a rising capture clock per fetch."""

    def __init__(self, pages: dict[str, bytes]):
        self.pages, self.calls = pages, []

    def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
        self.calls.append(url)
        body = self.pages[url]
        digest = hashlib.sha256(body).hexdigest()
        clock = datetime.fromisoformat(RUMOUR_PAGE_1_CAPTURED) + timedelta(seconds=len(self.calls))
        return FetchOutcome(
            status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
            raw_body_hash=digest, raw_fetched_at=clock.isoformat(), cache_hit=False,
            status_code=200)


def _rumour_run_plan(**targets):
    from scrapers.transfermarkt.orchestrator import RumourCompetitionTarget
    return CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t13', cycle_id='tm-cycle-t13',
        rumour_competitions=(RumourCompetitionTarget('GB1', 'Premier League', **targets),))


def test_rumour_walk_unions_the_links_of_every_fetched_page():
    # The first pager skips page 3; page 2 supplies that link.
    evidences = {1: (1, 2, 4), 2: (1, 2, 3, 4), 3: (1, 2, 3, 4), 4: (1, 2, 3, 4)}
    pages = {RUMOUR_LISTING: _rumour_feed_html(1, 4, evidences[1])}
    pages.update({_rumour_page_url(n): _rumour_feed_html(n, 4, numbers)
                  for n, numbers in evidences.items()})
    fetcher = _FeedFetch(pages)
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=RUMOUR_EXTRACTED_AT)
    db = CoreDB()
    report = run.run(_rumour_run_plan(), writer=CoreWriter(db), connection_factory=lambda: db)
    assert fetcher.calls == [RUMOUR_LISTING] + [_rumour_page_url(n) for n in (1, 2, 3, 4)]
    assert [item['status'] for item in report.outcomes] == ['observed_present'] * 4
    assert sum(item['rows'] for item in report.outcomes) == 4
    assert report.dq['passed'] is True and report.receipts[0]['verified'] is True
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_rumour_observations').fetchone()[0] == 4


def test_rumour_feed_no_fetched_page_completes_is_not_certified():
    # Every fetched pager omits page 3 of four: no complete feed, no rows.
    pages = {RUMOUR_LISTING: _rumour_feed_html(1, 4, (1, 2, 4))}
    pages.update({_rumour_page_url(n): _rumour_feed_html(n, 4, (1, 2, 4), last_control=True)
                  for n in (1, 2, 4)})
    fetcher = _FeedFetch(pages)
    report = CoreRun(fetcher.fetch, universe_version=UNIVERSE,
                     extracted_at=RUMOUR_EXTRACTED_AT).run(_rumour_run_plan())
    assert fetcher.calls == [RUMOUR_LISTING] + [_rumour_page_url(n) for n in (1, 2, 4)]
    outcome, = report.outcomes
    assert (outcome['status'], outcome['status_reason'], outcome['rows']) == (
        'error', 'rumours:incomplete_page_plan', 0)
    assert report.receipts == ()


def test_empty_rumour_pages_keep_their_capture_in_the_bundle():
    from scrapers.transfermarkt import rumours
    page_1 = _rumour_capture(1, RUMOUR_PAGE_1_CAPTURED)
    page_2 = _rumour_capture(2, RUMOUR_PAGE_2_CAPTURED, body=_no_entries_html(2, 2))
    outcomes = sections.extract_rumour_observations(
        (page_1, page_2), _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE)
    # One proof-bearing outcome per confirmed empty page: its capture joins
    # the bundle digest and the physical read-back instead of disappearing.
    assert [(outcome.status, outcome.capture.capture_id) for outcome in outcomes] == [
        ('observed_present', page_1.capture_id), ('observed_empty', page_2.capture_id)]
    assert (outcomes[1].proof, outcomes[1].rows) == ('rumours:no_entries_row', ())
    assert outcomes[1].parser_revision == rumours.PARSER_VERSION
    assert core.build_core_dq(outcomes)['passed'] is True

    db = CoreDB()
    plans = [core.plan_for_outcome(
        outcome, captured_at=outcome.capture.captured_at,
        scope_values={'competition_id': 'GB1'}) for outcome in outcomes]
    receipt = native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), plans, run_id=outcomes[0].run_id,
        universe_version=UNIVERSE, committed_at=RUMOUR_EXTRACTED_AT)
    assert receipt['verified'] is True
    assert [proof['rows'] for proof in receipt['plans']] == [1, 0]
    assert db.sql.execute(
        'SELECT source_capture_id, row_count, observation_status, parser_revision, schema_revision '
        'FROM transfermarkt_core_receipts_v1 ORDER BY rowid').fetchall() == [
        (page_1.capture_id, 1, 'observed_present', rumours.PARSER_VERSION, rumours.SCHEMA_VERSION),
        (page_2.capture_id, 0, 'observed_empty', rumours.PARSER_VERSION, rumours.SCHEMA_VERSION)]


def test_all_empty_rumour_feed_reports_each_page():
    empty_pages = (
        SourceCapture(url=_rumour_page_url(1), endpoint='rumours', body=_no_entries_html(1, 2),
                      capture_id='1' * 64, body_hash=hashlib.sha256(_no_entries_html(1, 2)).hexdigest(),
                      captured_at=RUMOUR_PAGE_1_CAPTURED),
        SourceCapture(url=_rumour_page_url(2), endpoint='rumours', body=_no_entries_html(2, 2),
                      capture_id='2' * 64, body_hash=hashlib.sha256(_no_entries_html(2, 2)).hexdigest(),
                      captured_at=RUMOUR_PAGE_2_CAPTURED),
    )
    outcomes = sections.extract_rumour_observations(
        empty_pages, _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE)
    assert [(outcome.status, outcome.capture.capture_id) for outcome in outcomes] == [
        ('observed_empty', empty_pages[0].capture_id),
        ('observed_empty', empty_pages[1].capture_id)]
    assert core.build_core_dq(outcomes)['passed'] is True


def test_rumour_discovery_always_takes_the_canonical_route():
    from scrapers.transfermarkt.orchestrator import planned_urls
    # Transfermarkt serves the grid under the generic "-" slug and its pager
    # links carry it; a competition slug fails the donor's navigation check.
    plan = _rumour_run_plan(slug='premier-league')
    assert planned_urls(plan) == (RUMOUR_LISTING,)
    empty_grid = (b'<div id="yw1"><span class="empty">No information available.</span>'
                  b'<div class="keys"></div></div>')
    report = CoreRun(_FeedFetch({RUMOUR_LISTING: empty_grid}).fetch, universe_version=UNIVERSE,
                     extracted_at=RUMOUR_EXTRACTED_AT).run(plan)
    outcome, = report.outcomes
    assert (outcome['status'], outcome['status_reason']) == ('observed_empty', 'rumours:empty_grid')


def test_rumour_scope_check_uses_the_donors_page_identity():
    def capture(url):
        body = _rumour_page_html(2)
        return SourceCapture(url=url, endpoint='rumours', body=body,
                             capture_id=hashlib.sha256(body).hexdigest(),
                             body_hash=hashlib.sha256(body).hexdigest(),
                             captured_at=RUMOUR_PAGE_2_CAPTURED)

    scope = {'competition_id': 'GB1'}
    for url in (RUMOUR_PAGE[2], RUMOUR_PAGE[2] + '/', RUMOUR_LISTING + '/plus/1/page/2',
                RUMOUR_LISTING + '?page=2', RUMOUR_LISTING):
        core._check_scope_against_capture('rumour_observations', scope, capture(url))
    for url in ('https://www.transfermarkt.com/-/geruechte/wettbewerb/ES1/page/2',
                'https://www.transfermarkt.com/-/tabelle/wettbewerb/GB1/page/2',
                'https://transfermarkt.example.com/-/geruechte/wettbewerb/GB1/page/2',
                RUMOUR_PAGE[2] + '#top'):
        with pytest.raises(CoreParseError, match='row_scope_mismatch'):
            core._check_scope_against_capture('rumour_observations', scope, capture(url))


# ------------------------------------------------ referee walk pins (r2 cycle)
REFEREE_DIRECTORY_PAGE_2 = REFEREE_DIRECTORY_URL + '/page/2'


def _referee_next_link(url: str) -> bytes:
    return f'<ul class="naechste-seite"><li><a href="{url}">Next</a></li></ul>'.encode()


def _referee_second_page_body() -> bytes:
    """The compact directory layout with two other referees on record."""
    body = _referee_first_page_body()
    return (body.replace(b'schiedsrichter/101', b'schiedsrichter/303')
                .replace(b'Ada Example', b'Cara Third')
                .replace(b'schiedsrichter/202', b'schiedsrichter/404')
                .replace(b'Boris Sample', b'Dan Fourth'))


def _referee_first_page_body() -> bytes:
    return (REFEREE_FIXTURES / 'directory_compact.html').read_bytes()


def _referee_page(url: str, body: bytes, *, next_url: str | None = None) -> SourceCapture:
    if next_url is not None:
        body += _referee_next_link(next_url)
    digest = hashlib.sha256(body).hexdigest()
    return SourceCapture(
        url=url, endpoint='referee_directory', body=body, capture_id=digest,
        body_hash=digest, captured_at=REFEREE_CAPTURED)


def _referee_directory(captures) -> tuple[LaneOutcome, ...]:
    return sections.extract_referee_directory_observations(
        captures, _referee_scope(), extracted_at=REFEREE_EXTRACTED_AT,
        universe_version=UNIVERSE)


def _directory_error(captures) -> LaneOutcome:
    outcome, = _referee_directory(captures)
    assert (outcome.status, outcome.rows) == ('error', ())
    return outcome


def test_truncated_referee_directory_is_not_certified():
    # Page 1 still links to page 2: page 1 alone is a truncation of the walk.
    page_1 = _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body(),
                           next_url=REFEREE_DIRECTORY_PAGE_2)
    outcome = _directory_error((page_1,))
    assert outcome.status_reason == 'referees:directory_pagination_incomplete'
    assert outcome.capture.capture_id == page_1.capture_id  # capture-backed
    assert core.build_core_dq([outcome])['passed'] is True


def test_referee_pages_out_of_walk_order_are_not_certified():
    page_1 = _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body(),
                           next_url=REFEREE_DIRECTORY_PAGE_2)
    page_2 = _referee_page(REFEREE_DIRECTORY_PAGE_2, _referee_second_page_body())
    page_3 = _referee_page(REFEREE_DIRECTORY_URL + '/page/3', _referee_second_page_body())
    # The supplied sequence must start at page 1 and stay contiguous.
    assert _directory_error((page_1, page_3)).status_reason == 'referees:directory_pagination_incomplete'
    assert _directory_error((page_2,)).status_reason == 'referees:directory_pagination_incomplete'
    # A page without its next link cannot be followed by another page.
    detached = _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body())
    assert _directory_error((detached, page_2)).status_reason == 'referees:directory_pagination_incomplete'


def test_malformed_referee_next_link_is_a_directory_error_of_its_own_page():
    page_1 = _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body(),
                           next_url=REFEREE_DIRECTORY_URL + '?page=99')
    outcome = _directory_error((page_1,))
    assert outcome.status_reason == 'referees:pagination_link_invalid'
    assert outcome.capture.capture_id == page_1.capture_id


def test_complete_referee_directory_walk_maps_each_page():
    page_1 = _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body(),
                           next_url=REFEREE_DIRECTORY_PAGE_2)
    page_2 = _referee_page(REFEREE_DIRECTORY_PAGE_2, _referee_second_page_body())
    outcomes = _referee_directory((page_1, page_2))
    assert [(outcome.status, len(outcome.rows)) for outcome in outcomes] == [
        ('observed_present', 10), ('observed_present', 10)]
    assert outcomes[1].capture.capture_id == page_2.capture_id
    assert core.build_core_dq(outcomes)['passed'] is True


def test_referee_directory_scope_run_writes_the_complete_walk():
    from scrapers.transfermarkt.orchestrator import RefereeDirectoryTarget
    bodies = {
        REFEREE_DIRECTORY_URL: _referee_first_page_body()
        + _referee_next_link(REFEREE_DIRECTORY_PAGE_2),
        REFEREE_DIRECTORY_PAGE_2: _referee_second_page_body(),
    }

    class _Fetch:
        def __init__(self):
            self.calls = []

        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            self.calls.append(url)
            body = bodies[url]
            digest = hashlib.sha256(body).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
                raw_body_hash=digest, raw_fetched_at=REFEREE_CAPTURED, cache_hit=False,
                status_code=200)

    fetcher = _Fetch()
    plan = CoreScopePlan(
        league='XY1', season='2526', scope_id='tm-scope-t12', cycle_id='tm-cycle-t12',
        referee_directories=(RefereeDirectoryTarget('XY1', '2025', 'example-league'),),
    )
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=REFEREE_EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert fetcher.calls == [REFEREE_DIRECTORY_URL, REFEREE_DIRECTORY_PAGE_2]
    assert [item['rows'] for item in report.outcomes] == [10, 10]
    assert report.dq['passed'] is True and len(report.receipts) == 1
    assert report.receipts[0]['verified'] is True
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_referee_aggregate_observations').fetchone()[0] == 20


def test_malformed_referee_next_link_fails_only_its_directory():
    from scrapers.transfermarkt.orchestrator import (
        AchievementHolderTarget, RefereeDirectoryTarget)
    # One run, two independent targets: a JSON holder and a broken directory.
    achievements_body = json.dumps(ACHIEVEMENTS_PACKET).encode('utf-8')
    directory_body = _referee_first_page_body() + _referee_next_link(REFEREE_DIRECTORY_URL + '?page=99')
    served = {
        ACHIEVEMENTS_URL: ('player_achievement', achievements_body, CLUB_CAPTURED),
        REFEREE_DIRECTORY_URL: ('referee_directory', directory_body, REFEREE_CAPTURED),
    }

    class _Fetch:
        def __init__(self):
            self.calls = []

        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            assert label == served[url][0]
            if label != 'referee_directory':
                return FetchOutcome(
                    status=FetchStatus.OK, value=ACHIEVEMENTS_PACKET,
                    raw_capture_id=hashlib.sha256(served[url][1]).hexdigest(),
                    raw_body_hash=hashlib.sha256(served[url][1]).hexdigest(),
                    raw_fetched_at=served[url][2], cache_hit=False, status_code=200)
            self.calls.append(url)
            body = served[url][1]
            return FetchOutcome(
                status=FetchStatus.OK, value=body.decode('utf-8'),
                raw_capture_id=hashlib.sha256(body).hexdigest(),
                raw_body_hash=hashlib.sha256(body).hexdigest(),
                raw_fetched_at=served[url][2], cache_hit=False, status_code=200)

        def raw_loader(self, capture_id):
            url, = [url for url, item in served.items()
                    if hashlib.sha256(item[1]).hexdigest() == capture_id]
            return served[url][1], _JsonRecord(url, capture_id, served[url][2])

    fetcher = _Fetch()
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        achievement_holders=(AchievementHolderTarget('player', '7'),),
        referee_directories=(RefereeDirectoryTarget('XY1', '2025', 'example-league'),),
    )
    run = CoreRun(fetcher.fetch, raw_loader=fetcher.raw_loader, universe_version=UNIVERSE,
                  extracted_at=REFEREE_EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    # The holder parsed before the broken directory keeps its write and receipt.
    assert [(item['entity'], item['status'], item['rows']) for item in report.outcomes] == [
        ('achievement_observations', 'observed_present', 2),
        ('referee_aggregate_observations', 'error', 0)]
    assert report.outcomes[1]['status_reason'] == 'referees:pagination_link_invalid'
    assert len(report.receipts) == 1 and report.receipts[0]['verified'] is True
    assert fetcher.calls == [REFEREE_DIRECTORY_URL]  # the broken link is not followed
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_achievement_observations').fetchone()[0] == 2
    # The broken directory produced no plan at all: not even its table.
    assert db.sql.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' "
        "AND name = 'transfermarkt_referee_aggregate_observations'").fetchone()[0] == 0


# --------------------------------------- review round-2 pins (findings 1-19)
from dataclasses import replace as _replace

ROWLESS_PAGE_1 = (
    b'<!doctype html><html><body><table class="items"><thead><tr><th>#</th><th>Stadium</th>'
    b'<th>Capacity</th><th>Spectators</th><th>Average</th><th>Matches</th><th>Sold out</th>'
    b'<th>Capacity</th></tr></thead><tbody></tbody></table><ul id="yw2">'
    b'<li class="naechste-seite"><a href="/premier-league/besucherzahlen/wettbewerb/GB1/'
    b'saison_id/2025?page=2">Next</a></li></ul></body></html>')


def _capture(url, body, captured_at, *, endpoint='attendance', capture_id=None):
    digest = hashlib.sha256(body).hexdigest()
    return SourceCapture(url=url, endpoint=endpoint, body=body, capture_id=capture_id or digest,
                         body_hash=digest, captured_at=captured_at)


def _stadiums(db):
    return dict(db.sql.execute(
        'SELECT club_id, stadium_name FROM transfermarkt_attendance_observations').fetchall())


def test_rowless_stale_page_cannot_delete_fresher_rows():
    # Round 2 finding 1: a page that contributes no row still carries a clock.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    older = _capture(ATTENDANCE_START, ROWLESS_PAGE_1, STALE_CAPTURED,
                     capture_id=hashlib.sha256(b'older rowless page').hexdigest())
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, [_attendance_plan((older, page_2), LATER_EXTRACTED_AT)], LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11', '631'}
    # A newer rowless page is a legitimate change of the edition.
    newer = _capture(ATTENDANCE_START, ROWLESS_PAGE_1, NEWER_PAGE_CAPTURED,
                     capture_id=hashlib.sha256(b'newer rowless page').hexdigest())
    receipt = _bundle(db, [_attendance_plan((newer, page_2), LATER_EXTRACTED_AT)], LATER_EXTRACTED_AT)
    assert receipt['verified'] is True
    assert set(_stadiums(db)) == {'631'}


def test_reused_capture_with_changed_rows_fails_when_a_sibling_page_changes():
    # Round 2 finding 2: a fresh page 2 must not switch off the integrity
    # check for the page 1 capture the snapshot still reuses.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    fresh_page_2 = _capture(ATTENDANCE_PAGE_2, page_2.body, NEWER_PAGE_CAPTURED,
                            capture_id=hashlib.sha256(b'fresh page 2').hexdigest())
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    plan = _attendance_plan((page_1, fresh_page_2), LATER_EXTRACTED_AT)
    plan = _replace(plan, rows=tuple(
        {**row, 'stadium_name': 'Ashburton Grove'} if row['source_url'] == ATTENDANCE_START else row
        for row in plan.rows))
    with pytest.raises(RuntimeError, match='replay differs'):
        _bundle(db, [plan], LATER_EXTRACTED_AT)
    assert _stadiums(db)['11'] == 'Emirates Stadium'


def test_snapshot_pages_come_from_one_capture_each():
    # Round 2 finding 3: inside a multi-page snapshot a source page is one capture.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    plan = _attendance_plan((page_1, page_2))
    # Page 2 reached through two captures: its own and page 1's.
    borrowed = {**plan.rows[0], 'source_url': ATTENDANCE_PAGE_2}
    with pytest.raises(ValueError, match='one capture per source page'):
        _replace(plan, rows=(borrowed, plan.rows[1]))
    # The walk's page evidence names every page the rows came from.
    with pytest.raises(ValueError, match='snapshot evidence'):
        _replace(plan, pages=((ATTENDANCE_PAGE_2, page_2.capture_id, PAGE_2_CAPTURED),))
    # A multi-page plan names the newest page's capture, or none at all.
    with pytest.raises(ValueError, match='newest row capture id'):
        _replace(plan, capture_id=page_1.capture_id)


def _empty_rumour_plans(captured_at):
    captures = (
        SourceCapture(url=_rumour_page_url(1), endpoint='rumours', body=_no_entries_html(1, 2),
                      capture_id='a' * 64, body_hash=hashlib.sha256(_no_entries_html(1, 2)).hexdigest(),
                      captured_at=captured_at),
        SourceCapture(url=_rumour_page_url(2), endpoint='rumours', body=_no_entries_html(2, 2),
                      capture_id='b' * 64, body_hash=hashlib.sha256(_no_entries_html(2, 2)).hexdigest(),
                      captured_at=captured_at),
    )
    outcomes = sections.extract_rumour_observations(
        captures, _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE)
    plans = [core.plan_for_outcome(
        outcome, captured_at=captured_at, scope_values={'competition_id': 'GB1'})
        for outcome in outcomes]
    return plans, outcomes[0].run_id


def _write(db, plans, run_id, committed_at):
    return native_writer.write_core_bundle(
        lambda: db, CoreWriter(db), plans, run_id=run_id, universe_version=UNIVERSE,
        committed_at=committed_at)


def test_empty_plans_of_one_bundle_keep_their_own_receipts():
    # Round 2 finding 4: two empty pages sharing a clock are two observations.
    db = CoreDB()
    plans, run_id = _empty_rumour_plans(RUMOUR_PAGE_1_CAPTURED)
    receipt = _write(db, plans, run_id, RUMOUR_EXTRACTED_AT)
    assert receipt['verified'] is True
    assert [row[0] for row in db.sql.execute(
            'SELECT source_capture_id FROM transfermarkt_core_receipts_v1 ORDER BY rowid')] == [
        'a' * 64, 'b' * 64]


def test_bundle_rejects_two_plans_with_one_receipt_identity_before_mutation():
    # Round 2 finding 4: one acknowledgement can never stand for two plans.
    db = CoreDB()
    plans, run_id = _empty_rumour_plans(RUMOUR_PAGE_1_CAPTURED)
    with pytest.raises(ValueError, match='duplicate core receipt'):
        _write(db, [plans[0], plans[0]], run_id, RUMOUR_EXTRACTED_AT)
    assert db.sql.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' "
        "AND name = 'transfermarkt_core_receipts_v1'").fetchone()[0] == 0


def _club_value_plans(fixture, stichtag, captured_at, extracted_at):
    body = (CLUB_VALUE_FIXTURES / fixture).read_bytes()
    capture = _capture(f'{CLUB_VALUE_URL}/stichtag/{stichtag}', body, captured_at,
                       endpoint='club_values')
    outcomes = sections.extract_club_value_observations(
        capture, _club_value_scope(), extracted_at=extracted_at, universe_version=UNIVERSE)
    return [core.plan_for_outcome(
        outcome, captured_at=captured_at, scope_values={'club_id': outcome.rows[0]['club_id']})
        for outcome in outcomes]


def test_reobserved_club_values_refresh_lineage_beside_a_newer_stichtag_capture():
    # Round 2 finding 6: two stichtag requests share clubs, so the older
    # capture's lineage refresh used to be refused against the newer clock.
    early = _club_value_plans('vie1-2013-04-15.html', '2013-04-15', PAGE_1_CAPTURED, EXTRACTED_AT)
    late = _club_value_plans('vie1-2024-06-01.html', '2024-06-01', PAGE_2_CAPTURED, EXTRACTED_AT)
    assert {plan.scope_values for plan in early} <= {plan.scope_values for plan in late}
    db = CoreDB()
    _bundle(db, early, EXTRACTED_AT)
    _bundle(db, late, EXTRACTED_AT)
    again_early = _club_value_plans('vie1-2013-04-15.html', '2013-04-15', PAGE_1_CAPTURED,
                                    LATER_EXTRACTED_AT)
    again_late = _club_value_plans('vie1-2024-06-01.html', '2024-06-01', PAGE_2_CAPTURED,
                                   LATER_EXTRACTED_AT)
    assert _bundle(db, again_early, LATER_EXTRACTED_AT)['verified'] is True
    assert _bundle(db, again_late, LATER_EXTRACTED_AT)['verified'] is True
    rows = db.sql.execute(
        'SELECT COUNT(*), COUNT(DISTINCT run_id) FROM transfermarkt_club_value_observations'
    ).fetchone()
    assert rows[1] == 1  # every row refreshed to the later run, none duplicated


_REFEREE_ROW = (
    '<tr class="{shade}"><td><table class="inline-table">'
    '<tr><td class="hauptlink"><a href="/x-{referee_id}/profil/schiedsrichter/{referee_id}">{name}</a></td></tr>'
    '<tr><td>Nowhere</td></tr></table></td><td class="zentriert">01/02/2020</td>'
    '<td class="zentriert">{matches}</td><td class="zentriert">61</td><td class="zentriert">2</td>'
    '<td class="zentriert">0</td><td class="zentriert">4</td></tr>')


def _referee_body(referees, *, matches_of=None, next_url=None):
    rows = ''.join(_REFEREE_ROW.format(
        shade='odd' if index % 2 else 'even', referee_id=referee_id, name=name,
        matches=(matches_of or {}).get(referee_id, '14'))
        for index, (referee_id, name) in enumerate(referees, 1))
    table = '<!DOCTYPE html><html><body><table class="items"><tbody>' + rows + '</tbody></table>'
    if next_url is not None:
        table += _referee_next_link(next_url).decode()
    return (table + '</body></html>').encode()


def _referee_plans(outcomes, captured_at_of):
    return [core.plan_for_outcome(
        outcome, captured_at=captured_at_of[outcome.capture.capture_id],
        scope_values={'competition_id': 'XY1', 'edition_id': '2025'}) for outcome in outcomes]


def test_merge_plan_cannot_take_a_natural_key_from_another_capture():
    # Round 2 finding 5: observed_at is shared by two captures of one page.
    first = _referee_page(REFEREE_DIRECTORY_URL, _referee_body([('101', 'Ada Example')]))
    db = CoreDB()
    _bundle(db, _referee_plans(_referee_directory((first,)), {first.capture_id: REFEREE_CAPTURED}),
            REFEREE_EXTRACTED_AT)
    other_body = _referee_body([('101', 'Ada Example')], matches_of={'101': '13'})
    other = SourceCapture(
        url=REFEREE_DIRECTORY_URL, endpoint='referee_directory', body=other_body,
        capture_id='c' * 64, body_hash=hashlib.sha256(other_body).hexdigest(),
        captured_at=REFEREE_CAPTURED)
    assert other.capture_id != first.capture_id
    plans = _referee_plans(_referee_directory((other,)), {other.capture_id: REFEREE_CAPTURED})
    with pytest.raises(RuntimeError, match='another capture'):
        _bundle(db, plans, REFEREE_EXTRACTED_AT)
    value = db.sql.execute(
        "SELECT value FROM transfermarkt_referee_aggregate_observations "
        "WHERE referee_id = '101' AND metric = 'matches_officiated'").fetchall()
    assert value == [('14',)]


def test_cached_directory_page_survives_a_first_page_ownership_shift():
    # Round 2 finding 7: a fresh page 1 may list a referee that the cached
    # page 2 owned during the previous walk.
    page_1 = _referee_page(REFEREE_DIRECTORY_URL,
                           _referee_body([('101', 'Ada Example'), ('202', 'Boris Sample')]),
                           next_url=REFEREE_DIRECTORY_PAGE_2)
    page_2 = _referee_page(REFEREE_DIRECTORY_PAGE_2,
                           _referee_body([('303', 'Cara Third'), ('404', 'Dan Fourth')]))
    db = CoreDB()
    _bundle(db, _referee_plans(_referee_directory((page_1, page_2)),
                               {page_1.capture_id: REFEREE_CAPTURED, page_2.capture_id: REFEREE_CAPTURED}),
            REFEREE_EXTRACTED_AT)
    later = '2026-11-20T16:00:00.000000+00:00'
    fresh_body = _referee_body([('101', 'Ada Example'), ('202', 'Boris Sample'), ('303', 'Cara Third')],
                               next_url=REFEREE_DIRECTORY_PAGE_2)
    fresh_1 = SourceCapture(
        url=REFEREE_DIRECTORY_URL, endpoint='referee_directory',
        body=fresh_body, capture_id='d' * 64,
        body_hash=hashlib.sha256(fresh_body).hexdigest(), captured_at=later)
    outcomes = _referee_directory((fresh_1, page_2))
    assert [len(outcome.rows) for outcome in outcomes] == [15, 5]
    plans = _referee_plans(outcomes, {fresh_1.capture_id: later, page_2.capture_id: REFEREE_CAPTURED})
    assert _bundle(db, plans, later)['verified'] is True
    # The earlier observation of referee 303 under page 2's capture is kept.
    rows = db.sql.execute(
        "SELECT source_capture_id, COUNT(*) FROM transfermarkt_referee_aggregate_observations "
        "WHERE referee_id = '303' GROUP BY 1 ORDER BY 1").fetchall()
    assert rows == sorted([(page_2.capture_id, 5), ('d' * 64, 5)])


# --------------------------------- review round-2 pins (findings 8-19)
def test_auto_extraction_clock_postdates_every_target_of_the_run():
    # Round 2 finding 8: the clock used to be fixed after the first target,
    # so the second target's own capture was newer than its extraction clock.
    bodies = dict(_attendance_pages())
    bodies[RUMOUR_LISTING] = (b'<div id="yw1"><span class="empty">No information available.</span>'
                              b'<div class="keys"></div></div>')

    class _Fetch:
        """Every capture is stamped with the wall clock of its own fetch."""
        def __init__(self):
            self.calls = []

        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            self.calls.append(url)
            body = bodies[url]
            digest = hashlib.sha256(body).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
                raw_body_hash=digest, raw_fetched_at=datetime.now(timezone.utc).isoformat(),
                cache_hit=False, status_code=200)

    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c', edition_id='2025',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),),
        rumour_competitions=(RumourCompetitionTarget('GB1', 'Premier League'),),
    )
    report = CoreRun(_Fetch().fetch, universe_version=UNIVERSE).run(plan)
    assert [(item['entity'], item['status']) for item in report.outcomes] == [
        ('attendance_observations', 'observed_present'), ('rumour_observations', 'observed_empty')]
    # One extraction clock and run identity per target, each sampled after
    # that target's last fetch.
    assert len({item['run_id'] for item in report.outcomes}) == 2
    assert [datetime.fromisoformat(item['extracted_at']) for item in report.outcomes] == sorted(
        datetime.fromisoformat(item['extracted_at']) for item in report.outcomes)


def test_conflicting_discovery_evidence_is_an_error_not_a_feed():
    # Round 2 finding 9: the listing advertises page 2, canonical page 1 only
    # itself. The walk sees the conflict; page 1 alone is not the feed.
    pages = {RUMOUR_LISTING: _rumour_feed_html(1, 2, (1, 2)),
             _rumour_page_url(1): _rumour_feed_html(1, 1, (1,))}
    fetcher = _FeedFetch(pages)
    report = CoreRun(fetcher.fetch, universe_version=UNIVERSE,
                     extracted_at=RUMOUR_EXTRACTED_AT).run(_rumour_run_plan())
    assert fetcher.calls == [RUMOUR_LISTING, _rumour_page_url(1)]
    outcome, = report.outcomes
    assert (outcome['status'], outcome['status_reason'], outcome['rows']) == (
        'error', 'rumours:conflicting_page_ref', 0)
    assert report.receipts == ()


def test_rumour_pages_are_assembled_in_plan_order():
    # Round 2 finding 10: page 3 reveals the link to page 2, so page 2 is
    # fetched after page 3 but is still the second page of the feed.
    evidences = {1: (1, 3, 4), 3: (1, 2, 3, 4), 2: (1, 2, 3, 4), 4: (1, 2, 3, 4)}
    pages = {RUMOUR_LISTING: _rumour_feed_html(1, 4, evidences[1])}
    pages.update({_rumour_page_url(n): _rumour_feed_html(n, 4, numbers)
                  for n, numbers in evidences.items()})
    fetcher = _FeedFetch(pages)
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=RUMOUR_EXTRACTED_AT)
    db = CoreDB()
    report = run.run(_rumour_run_plan(), writer=CoreWriter(db), connection_factory=lambda: db)
    assert fetcher.calls == [RUMOUR_LISTING] + [_rumour_page_url(n) for n in (1, 3, 2, 4)]
    assert [item['status'] for item in report.outcomes] == ['observed_present'] * 4
    assert report.dq['passed'] is True and report.receipts[0]['verified'] is True
    assert [row[0] for row in db.sql.execute(
        'SELECT page_number FROM transfermarkt_rumour_observations ORDER BY page_number')] == [1, 2, 3, 4]


def test_failed_club_value_requests_stay_distinct_by_stichtag():
    # Round 2 finding 18: two stichtag targets of one competition.
    class _Blocked:
        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error='waf challenge')

    plan = CoreScopePlan(
        league='VIE1', season='2526', scope_id='s', cycle_id='c',
        club_value_targets=(ClubValueTarget('VIE1', 'v-league-1', '2013-04-15'),
                            ClubValueTarget('VIE1', 'v-league-1', '2024-06-01')))
    report = CoreRun(_Blocked().fetch, universe_version=UNIVERSE,
                     extracted_at=EXTRACTED_AT).run(plan)
    assert report.dq['passed'] is True
    assert {item['subject'] for item in report.outcomes} == {
        '{"competition_id":"VIE1","stichtag":"2013-04-15"}',
        '{"competition_id":"VIE1","stichtag":"2024-06-01"}'}


def test_referee_walk_follows_the_exact_pages_the_links_name():
    # Round 2 finding 11: page 1 names a Wettbewerb page 2; a Pokalwettbewerb
    # page 2 with the same number is not the page the walk evidenced.
    page_1 = _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body(),
                           next_url=REFEREE_DIRECTORY_PAGE_2)
    elsewhere = _referee_page(REFEREE_DIRECTORY_URL.replace('/wettbewerb/', '/pokalwettbewerb/')
                              + '/page/2', _referee_second_page_body())
    outcome = _directory_error((page_1, elsewhere))
    assert outcome.status_reason == 'referees:directory_pagination_incomplete'


def test_unparsable_referee_next_link_is_contained():
    # Round 2 finding 12: the donor probe raises a plain ValueError here.
    page_1 = _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body(),
                           next_url='https://[bad')
    outcome = _directory_error((page_1,))
    assert (outcome.status_reason, outcome.capture.capture_id) == (
        'referees:pagination_link_invalid', page_1.capture_id)
    assert sections.extract_referee_profile_observations(
        _referee_profile_capture(), _referee_scope(), referee_id='101',
        extracted_at=REFEREE_EXTRACTED_AT, universe_version=UNIVERSE).status == 'observed_present'


def test_attendance_scope_check_follows_the_donor_page_identity():
    # Round 2 finding 13: the donor accepts a trailing slash before ?page=.
    page_1_body = (ATTENDANCE_FIXTURES / 'page_1.html').read_bytes().replace(
        b'saison_id/2025?page=2', b'saison_id/2025/?page=2')
    page_2_url = ATTENDANCE_START + '/?page=2'
    bodies = {ATTENDANCE_START: page_1_body, page_2_url: _attendance_pages()[ATTENDANCE_PAGE_2]}
    fetcher = _SectionFetch(bodies)
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='tm-scope-t10', cycle_id='tm-cycle-t10',
        edition_id='2025',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),),
    )
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    db = CoreDB()
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert fetcher.calls == [ATTENDANCE_START, page_2_url]
    assert (report.outcomes[0]['status'], report.outcomes[0]['rows']) == ('observed_present', 2)
    assert report.receipts[0]['verified'] is True
    for foreign in ('https://www.transfermarkt.com/premier-league/besucherzahlen/wettbewerb/ES1/saison_id/2025',
                    'https://www.transfermarkt.com/premier-league/besucherzahlen/wettbewerb/GB1/saison_id/2024',
                    'https://user@www.transfermarkt.com/premier-league/besucherzahlen/wettbewerb/GB1/saison_id/2025'):
        with pytest.raises(CoreParseError, match='row_scope_mismatch'):
            core._check_scope_against_capture(
                'attendance_observations', {'competition_id': 'GB1', 'edition_id': '2025'},
                _capture(foreign, b'<html></html>', PAGE_1_CAPTURED))


def test_empty_rumour_proof_names_the_layout_that_was_seen():
    # Round 2 finding 14: headers over an empty body carry no "no entries" row.
    header_only = ('<table class="items"><thead><tr><th>Player</th><th>Interested club</th>'
                   '<th>Most recent source from</th><th>Assessment</th></tr></thead>'
                   '<tbody></tbody></table><div class="pager"><ul>'
                   '<li class="tm-pagination__list-item tm-pagination__list-item--active">'
                   f'<a class="tm-pagination__link" href="{RUMOUR_LISTING}">1</a></li></ul></div>')
    capture = _capture(RUMOUR_LISTING, f'<div id="yw1">{header_only}</div>'.encode(),
                       RUMOUR_PAGE_1_CAPTURED, endpoint='rumours')
    outcome, = sections.extract_rumour_observations(
        (capture,), _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE)
    assert (outcome.status, outcome.proof) == ('observed_empty', 'rumours:empty_table_body')
    # The explicit donor marker keeps its own proof.
    marker = _capture(RUMOUR_PAGE[2], _no_entries_html(2, 2), RUMOUR_PAGE_2_CAPTURED, endpoint='rumours')
    first = _capture(RUMOUR_PAGE[1], _rumour_page_html(1), RUMOUR_PAGE_1_CAPTURED, endpoint='rumours')
    _, empty = sections.extract_rumour_observations(
        (first, marker), _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE)
    assert empty.proof == 'rumours:no_entries_row'


def test_unknown_presence_outcome_needs_an_unresolved_row():
    # Round 2 finding 17: every row explicit_none is the donor's explicit_none.
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        LaneOutcome('attendance_observations', 'unknown', 'attendance:rows_unresolved',
                    ({'observation_status': 'explicit_none'},), **_outcome_context())
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        LaneOutcome('club_value_observations', 'unknown', 'club_values:club_rows_unresolved',
                    ({'observation_status': 'explicit_none'},), **_outcome_context())


@pytest.mark.parametrize('lane, endpoint, url', [
    ('representation', 'representation', REPRESENTATION_URL),
    ('club_values', 'club_values', CLUB_VALUE_URL)])
def test_replay_adapters_keep_their_tuple_contract_on_http_failure(lane, endpoint, url):
    # Round 2 finding 19: a 404 capture answered a bare outcome, not a tuple.
    body = b'{"success": false}'
    failed = _capture(url, body, CLUB_CAPTURED, endpoint=endpoint)
    failed = _replace(failed, status_code=404)
    if lane == 'representation':
        outcomes = sections.extract_representation_observations(
            failed, _representation_scope(), expected_ids=('7',), extracted_at=EXTRACTED_AT,
            universe_version=UNIVERSE)
    else:
        outcomes = sections.extract_club_value_observations(
            failed, _club_value_scope(), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert isinstance(outcomes, tuple) and len(outcomes) == 1
    assert (outcomes[0].status, outcomes[0].rows) == ('unavailable', ())
    assert core.build_core_dq(outcomes)['passed'] is True


def test_revisions_are_rechecked_when_rows_change_after_construction():
    # Round 2 finding 16: the frozen outcome holds mutable row dictionaries.
    pages = (_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),
             _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED))
    outcome = sections.extract_attendance_observations(
        pages, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1', edition_id='2025',
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert core.build_core_dq([outcome])['passed'] is True
    outcome.rows[0]['parser_revision'] = 'wrong'
    assert 'revision_mismatch' in core.build_core_dq([outcome])['errors']
    with pytest.raises(CoreParseError, match='revision'):
        core.plan_for_outcome(outcome, captured_at=outcome.capture.captured_at,
                              scope_values={'competition_id': 'GB1', 'edition_id': '2025'})


def test_first_write_refuses_a_table_of_another_schema_before_mutation():
    # Round 2 finding 15: section lanes reuse the names of tables that the
    # retired donor stack left behind with its own columns.
    from scrapers.transfermarkt import referee_capture
    donor_columns = ', '.join(f'{column} TEXT' for column in referee_capture.REFEREE_PROFILE_COLUMNS)
    db = CoreDB()
    db.sql.execute(f'CREATE TABLE transfermarkt_referee_profiles ({donor_columns})')
    db.sql.execute("INSERT INTO transfermarkt_referee_profiles (referee_id, name) VALUES ('999', 'Old Row')")
    profile = sections.extract_referee_profile_observations(
        _referee_profile_capture(), _referee_scope(), referee_id='101',
        extracted_at=REFEREE_EXTRACTED_AT, universe_version=UNIVERSE)
    plan = core.plan_for_outcome(profile, captured_at=REFEREE_CAPTURED, scope_values={'referee_id': '101'})
    assert 'source_capture_id' not in referee_capture.REFEREE_PROFILE_COLUMNS
    with pytest.raises(RuntimeError, match='schema differs'):
        _write(db, [plan], profile.run_id, REFEREE_EXTRACTED_AT)
    assert db.sql.execute(
        'SELECT referee_id, name FROM transfermarkt_referee_profiles').fetchall() == [('999', 'Old Row')]

    # A table that merely carries one more column is refused just the same.
    packet = _representation_capture()
    assigned, = [outcome for outcome in sections.extract_representation_observations(
        packet, _representation_scope(), expected_ids=('7', '9'), extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE) if outcome.status == 'observed_present']
    columns = ', '.join(f'{column} TEXT' for column in (*assigned.rows[0], 'assessment_manifest_id'))
    db.sql.execute(f'CREATE TABLE transfermarkt_player_representation_observations ({columns})')
    plan = core.plan_for_outcome(assigned, captured_at=CLUB_CAPTURED, scope_values={'player_id': '7'})
    with pytest.raises(RuntimeError, match='schema differs'):
        _write(db, [plan], assigned.run_id, EXTRACTED_AT)
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_player_representation_observations').fetchone()[0] == 0


# --------------------------------- review round-3 pins (findings 1-23)
ROWLESS_TERMINAL_PAGE_2 = (
    b'<!doctype html><html><body><table class="items"><thead><tr><th>#</th><th>Stadium</th>'
    b'<th>Capacity</th><th>Spectators</th><th>Average</th><th>Matches</th><th>Sold out</th>'
    b'<th>Capacity</th></tr></thead><tbody></tbody></table></body></html>')


def _page(url, body, clock, name):
    return _capture(url, body, clock, capture_id=hashlib.sha256(name.encode()).hexdigest())


def test_replayed_snapshot_cannot_resurrect_a_page_a_newer_rowless_walk_removed():
    # Round 3 finding 1: the 12:30 empty page leaves no row to prove itself.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    original = [_attendance_plan((page_1, page_2))]
    db = CoreDB()
    _bundle(db, original, EXTRACTED_AT)
    # A newer walk saw an empty page 1: Arsenal legitimately leaves the edition.
    rowless_1 = _page(ATTENDANCE_START, ROWLESS_PAGE_1, NEWER_PAGE_CAPTURED, 'rowless 12:30')
    _bundle(db, [_attendance_plan((rowless_1, page_2), LATER_EXTRACTED_AT)], LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'631'}
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, original, EXTRACTED_AT)
    assert set(_stadiums(db)) == {'631'}


def test_newer_rowless_page_keeps_the_snapshot_fresh():
    # Round 3 finding 5: freshness is the newest WALKED page, rows or not.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    empty_chelsea = _page(ATTENDANCE_PAGE_2, ROWLESS_TERMINAL_PAGE_2, NEWER_PAGE_CAPTURED, 'empty 2')
    plan = _attendance_plan((page_1, empty_chelsea), LATER_EXTRACTED_AT)
    assert plan.captured_at == NEWER_PAGE_CAPTURED and plan.capture_id == page_1.capture_id
    assert _bundle(db, [plan], LATER_EXTRACTED_AT)['verified'] is True
    assert set(_stadiums(db)) == {'11'}


def test_reused_page_cannot_lose_its_rows_silently():
    # Round 3 finding 6: page 1 keeps its evidence but not its row.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    plan = _attendance_plan((page_1, page_2), LATER_EXTRACTED_AT)
    plan = _replace(plan, rows=tuple(row for row in plan.rows if row['club_id'] != '11'))
    with pytest.raises(RuntimeError, match='replay differs'):
        _bundle(db, [plan], LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11', '631'}


def test_rowless_plan_cannot_invent_its_capture_clock():
    # Round 3 finding 13: a rowless plan's clock is its capture's clock.
    outcome = sections.extract_achievement_observations(
        _achievement_capture(ACHIEVEMENTS_EMPTY), _achievement_scope(),
        recipient_type='player', recipient_id='7', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE)
    with pytest.raises(CoreParseError, match='captured_at'):
        core.plan_for_outcome(outcome, captured_at=LATER_EXTRACTED_AT,
                              scope_values={'recipient_type': 'player', 'recipient_id': '7'})
    plan = core.plan_for_outcome(outcome, captured_at=outcome.capture.captured_at,
                                 scope_values={'recipient_type': 'player', 'recipient_id': '7'})
    assert plan.captured_at == CLUB_CAPTURED


def _referee_at(url, body, capture_id, clock=REFEREE_CAPTURED):
    return SourceCapture(url=url, endpoint='referee_directory', body=body,
                         capture_id=capture_id, body_hash=hashlib.sha256(body).hexdigest(),
                         captured_at=clock)


def test_cached_page_cannot_gain_keys_from_another_capture_on_refresh():
    # Round 3 finding 2: both captures share a clock, so referee 101's keys
    # under page 2 would replace page 1's observation of the same keys.
    first_1 = _referee_at(REFEREE_DIRECTORY_URL, _referee_body([('101', 'Ada Example')],
                                                                next_url=REFEREE_DIRECTORY_PAGE_2), 'a' * 64)
    page_2 = _referee_at(REFEREE_DIRECTORY_PAGE_2, _referee_body(
        [('101', 'Ada Example'), ('202', 'Boris Sample')]), 'b' * 64)
    db = CoreDB()
    first = _referee_directory((first_1, page_2))
    assert [len(outcome.rows) for outcome in first] == [5, 5]
    _bundle(db, _referee_plans(first, {'a' * 64: REFEREE_CAPTURED, 'b' * 64: REFEREE_CAPTURED}),
            REFEREE_EXTRACTED_AT)
    # Next walk: a fresh page 1 no longer lists referee 101; page 2 is cached.
    fresh_1 = _referee_at(REFEREE_DIRECTORY_URL, _referee_body(
        [('303', 'Cara Third')], next_url=REFEREE_DIRECTORY_PAGE_2), 'c' * 64)
    second = sections.extract_referee_directory_observations(
        (fresh_1, page_2), _referee_scope(), extracted_at=LATER_EXTRACTED_AT, universe_version=UNIVERSE)
    assert [len(outcome.rows) for outcome in second] == [5, 10]
    with pytest.raises(RuntimeError, match='another capture'):
        _bundle(db, _referee_plans(second, {'c' * 64: REFEREE_CAPTURED, 'b' * 64: REFEREE_CAPTURED}),
                LATER_EXTRACTED_AT)
    owners = db.sql.execute(
        "SELECT source_capture_id, COUNT(*) FROM transfermarkt_referee_aggregate_observations "
        "WHERE referee_id = '101' GROUP BY 1").fetchall()
    assert owners == [('a' * 64, 5)]


def test_equal_clock_club_value_captures_cannot_take_each_others_keys():
    # Round 3 finding 7: two stichtag pages share clubs; at one clock their
    # keys collide, and at different clocks neither supersedes the other.
    early = _club_value_plans('vie1-2013-04-15.html', '2013-04-15', PAGE_1_CAPTURED, EXTRACTED_AT)
    same_clock = _club_value_plans('vie1-2024-06-01.html', '2024-06-01', PAGE_1_CAPTURED, EXTRACTED_AT)
    db = CoreDB()
    _bundle(db, early, EXTRACTED_AT)
    before = db.sql.execute('SELECT COUNT(*) FROM transfermarkt_club_value_observations').fetchone()[0]
    with pytest.raises(RuntimeError, match='another capture'):
        _bundle(db, same_clock, EXTRACTED_AT)
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_club_value_observations').fetchone()[0] == before

    later = _club_value_plans('vie1-2024-06-01.html', '2024-06-01', PAGE_2_CAPTURED, EXTRACTED_AT)
    earlier = _club_value_plans('vie1-2013-04-15.html', '2013-04-15', PAGE_1_CAPTURED, EXTRACTED_AT)
    db = CoreDB()
    _bundle(db, later, EXTRACTED_AT)
    assert _bundle(db, earlier, EXTRACTED_AT)['verified'] is True


def test_rumour_capture_cannot_gain_rows_on_replay():
    # Round 3 finding 8: the same capture replayed under another competition
    # display name hashes into other assertions.
    from scrapers.transfermarkt import rumours
    pages = (_rumour_capture(1, RUMOUR_PAGE_1_CAPTURED), _rumour_capture(2, RUMOUR_PAGE_2_CAPTURED))
    later = '2026-11-20T21:45:00.000000+00:00'

    def plans(extracted_at, name):
        outcomes = sections.extract_rumour_observations(
            pages, _rumour_scope(), competition=rumours.CompetitionContext(
                'GB1', name, 'https://www.transfermarkt.com/-/startseite/wettbewerb/GB1'),
            extracted_at=extracted_at, universe_version=UNIVERSE)
        return [core.plan_for_outcome(
            outcome, captured_at=outcome.capture.captured_at,
            scope_values={'competition_id': 'GB1'}) for outcome in outcomes]

    db = CoreDB()
    _bundle(db, plans(RUMOUR_EXTRACTED_AT, 'Premier League'), RUMOUR_EXTRACTED_AT)
    with pytest.raises(RuntimeError, match='replay differs'):
        _bundle(db, plans(later, 'English Premier League'), later)
    assert db.sql.execute(
        'SELECT COUNT(*) FROM transfermarkt_rumour_observations').fetchone()[0] == 2


def test_observation_clock_must_equal_its_capture_clock():
    # Round 3 finding 12: a row cannot claim an older observation clock than
    # the capture that carries it.
    first = _referee_page(REFEREE_DIRECTORY_URL, _referee_body([('101', 'Ada Example')]))
    db = CoreDB()
    _bundle(db, _referee_plans(_referee_directory((first,)), {first.capture_id: REFEREE_CAPTURED}),
            REFEREE_EXTRACTED_AT)
    later = '2026-11-20T15:07:00.000000+00:00'
    other = _referee_at(REFEREE_DIRECTORY_URL, _referee_body(
        [('101', 'Ada Example')], matches_of={'101': '13'}), 'e' * 64, later)
    outcome, = _referee_directory((other,))
    outcome = _replace(outcome, rows=tuple(
        {**row, 'observed_at': REFEREE_CAPTURED} for row in outcome.rows))
    plan = core.plan_for_outcome(outcome, captured_at=later,
                                 scope_values={'competition_id': 'XY1', 'edition_id': '2025'})
    with pytest.raises((ValueError, RuntimeError)):
        _bundle(db, [plan], later)
    assert db.sql.execute(
        "SELECT value FROM transfermarkt_referee_aggregate_observations "
        "WHERE referee_id = '101' AND metric = 'matches_officiated'").fetchall() == [('14',)]


def test_plan_freezes_rows_and_rechecks_row_verdicts():
    # Round 3 finding 11: the outcome holds mutable rows.
    pages = (_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),
             _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED))
    outcome = sections.extract_attendance_observations(
        pages, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1', edition_id='2025',
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    for row in outcome.rows:
        row['observation_status'] = 'unknown'
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        core.plan_for_outcome(outcome, captured_at=PAGE_2_CAPTURED,
                              scope_values={'competition_id': 'GB1', 'edition_id': '2025'})
    # A plan keeps the rows it validated.
    frozen = _attendance_plan(pages)
    outcome = sections.extract_attendance_observations(
        pages, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1', edition_id='2025',
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    plan = core.plan_for_outcome(outcome, captured_at=PAGE_2_CAPTURED,
                                 scope_values={'competition_id': 'GB1', 'edition_id': '2025'})
    outcome.rows[0]['parser_revision'] = 'wrong'
    assert {row['parser_revision'] for row in plan.rows} == {frozen.rows[0]['parser_revision']}
    assert plan.plan_sha256() == frozen.plan_sha256()


def test_unrepresentable_count_never_reaches_mutation():
    # Round 3 finding 4: a bigint column cannot hold this capacity; the
    # replacement must stop before it deletes the committed edition.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    overflow = page_1.body.replace(b'60.704', b'9223372036854775808')
    newer_1 = _page(ATTENDANCE_START, overflow, NEWER_PAGE_CAPTURED, 'overflow')
    with pytest.raises(ValueError, match='bigint'):
        _attendance_plan((newer_1, page_2), LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11', '631'}


def test_schema_preflight_checks_column_types_before_replacing():
    # Round 3 finding 9: same column names, another type.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    db.sql.execute('DROP TABLE transfermarkt_attendance_observations')
    # The fake warehouse is sqlite: the Iceberg DDL without its catalog prefix and format.
    db.sql.execute(core.core_ddl('attendance_observations').split(' WITH ')[0].replace(
        'iceberg.bronze.', '').replace('capacity bigint', 'capacity varchar'))
    db.sql.execute("INSERT INTO transfermarkt_attendance_observations (club_id) VALUES ('11')")
    newer_1 = _page(ATTENDANCE_START, page_1.body, NEWER_PAGE_CAPTURED, 'newer 1')
    with pytest.raises(RuntimeError, match='schema differs'):
        _bundle(db, [_attendance_plan((newer_1, page_2), LATER_EXTRACTED_AT)], LATER_EXTRACTED_AT)
    assert db.sql.execute(
        'SELECT club_id FROM transfermarkt_attendance_observations').fetchall() == [('11',)]
