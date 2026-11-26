"""Stage-C section lanes: attendance (T10), club values (T11), achievements (T8) (#2405)."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import pytest

from scrapers.transfermarkt import core, native_writer, sections
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
    outcome, = sections.extract_attendance_observations(
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
    outcome, = sections.extract_attendance_observations(
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
    outcome, = sections.extract_attendance_observations(
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
        capture, _club_value_scope(), competition_id='VIE1', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE)
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
        capture, _club_value_scope(), competition_id='VIE1', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE)
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
                           (_stub_row('attendance_observations', 'unknown'),), **context)
    assert accepted.status == 'unknown'
    # mixed per-row statuses stay valid under an observed_present outcome
    mixed = LaneOutcome('attendance_observations', 'observed_present', 'attendance:ok',
                        ({'observation_status': 'observed_present', 'spectators_total': 1},
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
    outcome, = sections.extract_attendance_observations(
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
    outcome, = sections.extract_achievement_observations(
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
    outcome, = sections.extract_achievement_observations(
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
    outcome, = sections.extract_achievement_observations(
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
    outcome, = sections.extract_referee_profile_observations(
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


@pytest.mark.parametrize('case', ['aliases', 'same_competition', 'body_conflict'])
def test_referee_capture_identity_includes_the_requested_competition(case):
    # Round 10 finding 13
    from scrapers.transfermarkt.orchestrator import RefereeDirectoryTarget

    page = _referee_directory_capture()
    competition_ids = ('XY1', 'XY1' if case == 'same_competition' else 'XY2')
    plan = CoreScopePlan(
        league='XY1', season='2526', scope_id='s', cycle_id='c', edition_id='2025',
        referee_directories=tuple(RefereeDirectoryTarget(cid, '2025', 'example-league')
                                  for cid in competition_ids),
    )
    first, second = planned_urls(plan)
    replay = _SectionFetch({first: page.body, second: page.body + (b' ' if case == 'body_conflict' else b'')})

    def fetch(url, **kwargs):
        return _replace(replay.fetch(url, **kwargs), raw_capture_id=page.capture_id)

    run = CoreRun(fetch, extracted_at=REFEREE_EXTRACTED_AT)
    if case == 'body_conflict':
        with pytest.raises(CoreRunError, match='^capture_evidence_conflict$'):
            run.run(plan, writer=object(), connection_factory=lambda: pytest.fail('unexpected connection'))
    elif case == 'same_competition':
        assert run.run(plan).dq['errors'] == ['duplicate_natural_key', 'duplicate_outcome']
    else:
        db = CoreDB()
        report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
        assert report.dq['errors'] == []
        assert len(report.receipts) == 2 and all(receipt['verified'] for receipt in report.receipts)
        receipts = db.sql.execute('SELECT scope_values_json, source_capture_id FROM transfermarkt_core_receipts_v1')
        assert {(dict(json.loads(scope))['competition_id'], dict(json.loads(scope))['edition_id'], cid)
                for scope, cid in receipts} == {(comp, '2025', page.capture_id) for comp in competition_ids}


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
    outcome, = sections.extract_achievement_observations(
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
    outcome, = sections.extract_attendance_observations(
        captures, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
        edition_id='2025', extracted_at=extracted_at, universe_version=UNIVERSE)
    # The plan clock is the snapshot's freshness: its newest walked page.
    return core.plan_for_outcome(
        outcome, captured_at=max(page[2] for page in outcome.pages),
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


def _stub_row(entity, verdict):
    """A row stub that carries the facts its verdict claims."""
    facts = {
        ('attendance_observations', 'observed_present'): {'spectators_total': 1},
        ('attendance_observations', 'unknown'): {'spectators_total_raw': '?'},
        ('referee_aggregate_observations', 'observed_present'): {
            'value': '14', 'raw_value': '14', 'status_reason': 'referees:numeric'},
        ('referee_aggregate_observations', 'observed_empty'): {'raw_value': ''},
        ('referee_aggregate_observations', 'explicit_none'): {'raw_value': '-'},
    }
    return {'observation_status': verdict, **facts.get((entity, verdict), {})}


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
    rows = tuple(_stub_row(entity, verdict) for verdict in verdicts)
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
    outcome, = sections.extract_achievement_observations(
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
    outcome, = sections.extract_attendance_observations(
        (_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),),
        _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1', edition_id='2025',
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert outcome.parser_revision == attendance.ATTENDANCE_PARSER_REVISION
    assert outcome.schema_revision == attendance.ATTENDANCE_SCHEMA_REVISION
    # An outcome cannot claim a revision its rows do not carry.
    with pytest.raises(CoreParseError, match='revision'):
        LaneOutcome('attendance_observations', 'observed_present', 'attendance:ok',
                    ({'observation_status': 'observed_present', 'spectators_total': 1,
                      'parser_revision': 'other-v9'},),
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
        _replace(plan, pages=((ATTENDANCE_PAGE_2, page_2.capture_id, PAGE_2_CAPTURED,
                               page_2.body_hash, ''),))
    # A multi-page plan names the newest page's capture, or none at all (its
    # evidence may not describe another page than the one it names: round 6).
    with pytest.raises(ValueError, match='newest row capture id|evidence differs from its page'):
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
        capture, _club_value_scope(), competition_id='VIE1', extracted_at=extracted_at,
        universe_version=UNIVERSE)
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
    # Round 3 named the precise reason: it is another directory altogether.
    assert outcome.status_reason == 'referees:directory_identity_invalid'


def test_unparsable_referee_next_link_is_contained():
    # Round 2 finding 12: the donor probe raises a plain ValueError here.
    page_1 = _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body(),
                           next_url='https://[bad')
    outcome = _directory_error((page_1,))
    assert (outcome.status_reason, outcome.capture.capture_id) == (
        'referees:pagination_link_invalid', page_1.capture_id)
    assert sections.extract_referee_profile_observations(
        _referee_profile_capture(), _referee_scope(), referee_id='101',
        extracted_at=REFEREE_EXTRACTED_AT, universe_version=UNIVERSE)[0].status == 'observed_present'


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
            failed, _club_value_scope(), competition_id='VIE1', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert isinstance(outcomes, tuple) and len(outcomes) == 1
    assert (outcomes[0].status, outcomes[0].rows) == ('unavailable', ())
    assert core.build_core_dq(outcomes)['passed'] is True


def test_revisions_are_rechecked_when_rows_change_after_construction():
    # Round 2 finding 16: the frozen outcome holds mutable row dictionaries.
    pages = (_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),
             _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED))
    outcome, = sections.extract_attendance_observations(
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
    profile, = sections.extract_referee_profile_observations(
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
    outcome, = sections.extract_achievement_observations(
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
    with pytest.raises((ValueError, RuntimeError)):
        plan = core.plan_for_outcome(outcome, captured_at=later,
                                     scope_values={'competition_id': 'XY1', 'edition_id': '2025'})
        _bundle(db, [plan], later)
    assert db.sql.execute(
        "SELECT value FROM transfermarkt_referee_aggregate_observations "
        "WHERE referee_id = '101' AND metric = 'matches_officiated'").fetchall() == [('14',)]


def test_plan_freezes_rows_and_rechecks_row_verdicts():
    # Round 3 finding 11: the outcome holds mutable rows.
    pages = (_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),
             _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED))
    outcome, = sections.extract_attendance_observations(
        pages, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1', edition_id='2025',
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    for row in outcome.rows:
        row['observation_status'] = 'unknown'
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        core.plan_for_outcome(outcome, captured_at=PAGE_2_CAPTURED,
                              scope_values={'competition_id': 'GB1', 'edition_id': '2025'})
    # A plan keeps the rows it validated.
    frozen = _attendance_plan(pages)
    outcome, = sections.extract_attendance_observations(
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
    # Round 4 finding 15: the adapter names the value the row cannot hold
    # instead of raising, so no plan is built from it.
    outcome, = sections.extract_attendance_observations(
        (newer_1, page_2), _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
        edition_id='2025', extracted_at=LATER_EXTRACTED_AT, universe_version=UNIVERSE)
    assert (outcome.status, outcome.status_reason, outcome.rows) == (
        'error', 'attendance:row_value_exceeds_bigint', ())
    with pytest.raises(ValueError, match='non-conclusive'):
        core.plan_for_outcome(outcome, captured_at=NEWER_PAGE_CAPTURED,
                              scope_values={'competition_id': 'GB1', 'edition_id': '2025'})
    # The plan boundary keeps its own check for a row built any other way.
    plan = _attendance_plan((page_1, page_2), LATER_EXTRACTED_AT)
    with pytest.raises(ValueError, match='bigint'):
        _replace(plan, rows=tuple(
            {**row, 'capacity': 2 ** 63} if row['club_id'] == '11' else row for row in plan.rows))
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


def test_edition_replacement_needs_the_whole_walk_from_its_first_page():
    # Round 3 finding 3: a terminal page offered as the start of the walk is
    # not an edition, and neither are captures the walk never visits.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)

    def extract(captures, start_url):
        outcome, = sections.extract_attendance_observations(
            captures, _attendance_scope(), start_url=start_url, competition_id='GB1',
            edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
        return outcome

    outcome = extract((page_2,), ATTENDANCE_PAGE_2)
    assert (outcome.status, outcome.status_reason, outcome.rows) == (
        'error', 'attendance:pagination_start_not_first_page', ())
    stray = _page(ATTENDANCE_PAGE_2.replace('?page=2', '?page=3'), page_2.body, PAGE_2_CAPTURED, 'stray')
    outcome = extract((page_1, page_2, stray), ATTENDANCE_START)
    assert (outcome.status, outcome.status_reason, outcome.rows) == (
        'error', 'attendance:pagination_captures_differ', ())
    assert extract((page_1, page_2), ATTENDANCE_START).status == 'observed_present'


def test_duplicate_replacement_targets_are_rejected_before_any_fetch():
    # Round 3 finding 14: two replacements of one scope inside a run.
    from scrapers.transfermarkt.orchestrator import ClubTarget
    class _Never:
        calls = []

        def fetch(self, url, **kwargs):
            self.calls.append(url)
            raise AssertionError('no fetch expected')

    editions = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c', edition_id='2025',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),
                             AttendanceEditionTarget('GB1', 2025, 'premier-league')))
    clubs = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        clubs=(ClubTarget('11', 'arsenal', squad_url='https://www.transfermarkt.com/x/kader/verein/11'),
               ClubTarget('11', 'arsenal', squad_url='https://www.transfermarkt.com/x/kader/verein/11')))
    for plan in (editions, clubs):
        with pytest.raises(CoreRunError, match='duplicate'):
            CoreRun(_Never().fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT).run(plan)
    assert _Never.calls == []


def test_club_value_lane_uses_the_target_competition_not_the_enclosing_league():
    # Round 3 finding 15: a GB1 scope may request a VIE1 stichtag page.
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        club_value_targets=(ClubValueTarget('VIE1', 'v-league-1'),))
    fetcher = _SectionFetch({CLUB_VALUE_URL: (CLUB_VALUE_FIXTURES / 'vie1-current.html').read_bytes()})
    report = CoreRun(fetcher.fetch, universe_version=UNIVERSE,
                     extracted_at=EXTRACTED_AT).run(plan)
    assert report.outcomes and {item['status'] for item in report.outcomes} == {'observed_present'}


def test_section_captures_must_come_from_transfermarkt():
    # Round 3 finding 16: the path alone does not identify the source.
    elsewhere = 'https://example.invalid'
    profile = _replace(_referee_profile_capture(), url=REFEREE_PROFILE_URL.replace(
        'https://www.transfermarkt.com', elsewhere))
    outcome, = sections.extract_referee_profile_observations(
        profile, _referee_scope(), referee_id='101', extracted_at=REFEREE_EXTRACTED_AT,
        universe_version=UNIVERSE)
    assert (outcome.status, outcome.rows) == ('error', ())
    assert outcome.status_reason == 'referees:row_scope_mismatch'
    directory = _replace(
        _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body()),
        url=REFEREE_DIRECTORY_URL.replace('https://www.transfermarkt.com', elsewhere))
    outcome = _directory_error((directory,))
    assert outcome.status_reason == 'referees:directory_identity_invalid'
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    moved = tuple(_replace(page, url=page.url.replace('https://www.transfermarkt.com', elsewhere))
                  for page in (page_1, page_2))
    outcome, = sections.extract_attendance_observations(
        moved, _attendance_scope(), start_url=moved[0].url, competition_id='GB1', edition_id='2025',
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert (outcome.status, outcome.rows) == ('error', ())
    assert outcome.status_reason == 'attendance:pagination_identity_invalid'


def test_representation_packet_must_answer_the_whole_request():
    # Round 3 finding 17: the capture asked for 7, 9 and 10; the caller
    # narrowed the expectation to the two players the body happened to hold.
    from scrapers.transfermarkt.orchestrator import representation_url
    body = json.dumps(REPRESENTATION_PACKET).encode('utf-8')
    digest = hashlib.sha256(body).hexdigest()
    capture = SourceCapture(url=representation_url(('7', '9', '10')), endpoint='representation',
                            body=body, capture_id=digest, body_hash=digest, captured_at=CLUB_CAPTURED)
    outcomes = sections.extract_representation_observations(
        capture, _representation_scope(), expected_ids=('7', '9'), extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE)
    assert [outcome.status for outcome in outcomes] == ['error']
    assert outcomes[0].status_reason == 'representation:requested_ids_differ'
    assert outcomes[0].rows == ()


def test_contract_cell_mapping_and_page_verdicts_are_checked_both_ways():
    # Round 3 finding 18: a parsed-error contract cell is never "present",
    # and a populated directory page is an observed page, not an absence.
    context = _outcome_context('player_contract_observations')
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        LaneOutcome('player_contract_observations', 'observed_present', 'squad_page',
                    ({'observation_status': 'observed_present', 'contract_status': 'parse_error'},),
                    **context)
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        LaneOutcome('referee_aggregate_observations', 'explicit_none', 'referees:dashes',
                    ({'observation_status': 'explicit_none'},), **_outcome_context())


def test_failed_rumour_walk_cites_the_page_that_failed():
    # Round 3 finding 19: the error outcome names the unreadable page.
    broken = b'<html><body>not a grid</body></html>'
    pages = {RUMOUR_LISTING: _rumour_feed_html(1, 2, (1, 2)),
             _rumour_page_url(1): _rumour_feed_html(1, 2, (1, 2)),
             _rumour_page_url(2): broken}
    report = CoreRun(_FeedFetch(pages).fetch, universe_version=UNIVERSE,
                     extracted_at=RUMOUR_EXTRACTED_AT).run(_rumour_run_plan())
    outcome, = report.outcomes
    assert (outcome['status'], outcome['status_reason']) == ('error', 'rumours:missing_table')
    assert outcome['source_capture_id'] == hashlib.sha256(broken).hexdigest()


def test_empty_rumour_proof_ignores_a_marker_outside_the_rumour_table():
    # Round 3 finding 20: another table on the page is not the rumour grid.
    unrelated = ('<div id="yw1"><table class="items"><thead><tr><th>Player</th>'
                 '<th>Interested club</th><th>Most recent source from</th><th>Assessment</th></tr>'
                 '</thead><tbody></tbody></table><table class="other"><tr>'
                 '<td class="no-entries">No entries available</td></tr></table></div>'
                 '<div class="pager"><ul><li class="tm-pagination__list-item '
                 'tm-pagination__list-item--active"><a class="tm-pagination__link" '
                 f'href="{RUMOUR_LISTING}">1</a></li></ul></div>')
    capture = _capture(RUMOUR_LISTING, unrelated.encode(), RUMOUR_PAGE_1_CAPTURED, endpoint='rumours')
    outcome, = sections.extract_rumour_observations(
        (capture,), _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE)
    assert outcome.proof == 'rumours:empty_table_body'


def test_directory_continuation_of_another_competition_is_a_directory_error():
    # Round 3 finding 21: the offending capture backs the error instead of
    # an exception leaving the adapter.
    page_1 = _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body(),
                           next_url=REFEREE_DIRECTORY_PAGE_2)
    elsewhere = REFEREE_DIRECTORY_PAGE_2.replace('/XY1/', '/ZZ1/')
    page_2 = _referee_page(elsewhere, _referee_second_page_body())
    outcome = _directory_error((page_1, page_2))
    assert (outcome.status_reason, outcome.capture.capture_id) == (
        'referees:directory_identity_invalid', page_2.capture_id)


def test_report_run_id_names_the_target_of_its_receipt():
    # Round 3 finding 22: the first outcome was a failed target.
    empty_grid = (b'<div id="yw1"><span class="empty">No information available.</span>'
                  b'<div class="keys"></div></div>')

    class _Fetch:
        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            if url != RUMOUR_LISTING:
                return FetchOutcome(status=FetchStatus.RETRY_EXHAUSTED, error='waf challenge')
            digest = hashlib.sha256(empty_grid).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, value=empty_grid.decode('utf-8'), raw_capture_id=digest,
                raw_body_hash=digest, raw_fetched_at=RUMOUR_PAGE_1_CAPTURED, cache_hit=False,
                status_code=200)

    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c', edition_id='2025',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),),
        rumour_competitions=(RumourCompetitionTarget('GB1', 'Premier League'),))
    db = CoreDB()
    report = CoreRun(_Fetch().fetch, universe_version=UNIVERSE).run(
        plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert [item['status'] for item in report.outcomes] == ['unavailable', 'observed_empty']
    assert report.run_id == report.receipt['run_id'] == report.outcomes[1]['run_id']


@pytest.mark.parametrize('lane', ['attendance', 'referee_profile', 'achievement'])
def _scalar_adapter(lane, failed):
    """Call the adapter of a single-outcome section; ``failed`` answers 404."""
    body = b'<html></html>'
    if lane == 'attendance':
        capture = _capture(ATTENDANCE_START, body if failed else (
            ATTENDANCE_FIXTURES / 'single_page.html').read_bytes(), PAGE_1_CAPTURED)
        return sections.extract_attendance_observations(
            (_replace(capture, status_code=404) if failed else capture,), _attendance_scope(),
            start_url=ATTENDANCE_START, competition_id='GB1', edition_id='2025',
            extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    if lane == 'referee_profile':
        capture = _capture(REFEREE_PROFILE_URL, body if failed else (
            REFEREE_FIXTURES / 'profile.html').read_bytes(), REFEREE_CAPTURED,
            endpoint='referee_profile')
        return sections.extract_referee_profile_observations(
            _replace(capture, status_code=404) if failed else capture, _referee_scope(),
            referee_id='101', extracted_at=REFEREE_EXTRACTED_AT, universe_version=UNIVERSE)
    capture = _achievement_capture(ACHIEVEMENTS_EMPTY if failed else ACHIEVEMENTS_PACKET)
    return sections.extract_achievement_observations(
        _replace(capture, status_code=404) if failed else capture, _achievement_scope(),
        recipient_type='player', recipient_id='7', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE)


@pytest.mark.parametrize('lane', ['attendance', 'referee_profile', 'achievement'])
def test_single_outcome_adapters_answer_a_one_element_tuple_on_every_path(lane):
    # Round 4 finding 21 (revises the round-3 pin): these adapters used to
    # answer a bare outcome, which does not compose with DQ.
    for failed in (False, True):
        outcomes = _scalar_adapter(lane, failed)
        assert isinstance(outcomes, tuple) and len(outcomes) == 1
        assert isinstance(outcomes[0], core.LaneOutcome)
        # The direct call the contract promises.
        assert core.build_core_dq(outcomes)['passed'] is True
        assert outcomes[0].status == ('unavailable' if failed else 'observed_present')


# --------------------------------- review round-4 pins (findings 1-21)

def test_one_capture_id_cannot_name_two_responses():
    # Round 4 finding 13: a by-id lookup used to attribute Arsenal to
    # Chelsea's URL, body hash and clock when both pages shared a capture id.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    same_id = _replace(page_2, capture_id=page_1.capture_id)
    with pytest.raises(CoreParseError, match='attendance:capture_conflict'):
        sections.extract_attendance_observations(
            (page_1, same_id), _attendance_scope(), start_url=ATTENDANCE_START,
            competition_id='GB1', edition_id='2025', extracted_at=EXTRACTED_AT,
            universe_version=UNIVERSE)
    rumour_1 = _rumour_capture(1, RUMOUR_PAGE_1_CAPTURED)
    rumour_2 = _replace(_rumour_capture(2, RUMOUR_PAGE_2_CAPTURED), capture_id=rumour_1.capture_id)
    with pytest.raises(CoreParseError, match='rumours:capture_conflict'):
        sections.extract_rumour_observations(
            (rumour_1, rumour_2), _rumour_scope(), competition=_rumour_competition(),
            extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE)


def test_row_lineage_must_match_its_own_page():
    # Round 4 finding 13: a row edited to carry another page's capture
    # evidence used to pass DQ and land in a verified receipt.
    pages = (_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),
             _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED))
    outcome, = sections.extract_attendance_observations(
        pages, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
        edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    arsenal = next(row for row in outcome.rows if row['club_id'] == '11')
    borrowed = tuple(
        {**row, 'raw_capture_id': arsenal['raw_capture_id'],
         'source_body_hash': arsenal['source_body_hash']}
        if row['club_id'] == '631' else row for row in outcome.rows)
    with pytest.raises(ValueError, match='lineage differs|names two captures'):
        core.plan_for_outcome(
            _replace(outcome, rows=borrowed), captured_at=PAGE_2_CAPTURED,
            scope_values={'competition_id': 'GB1', 'edition_id': '2025'})


def test_edition_replacement_requires_the_whole_page_manifest():
    # Round 4 finding 4: a terminal page's row without page evidence used to
    # replace the whole edition; a manifest may not start past the first page.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    scope_values = {'competition_id': 'GB1', 'edition_id': '2025'}

    def extract(captures):
        outcome, = sections.extract_attendance_observations(
            captures, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
            edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
        return outcome

    outcome = extract((page_1, page_2))

    def terminal_only(**changes):
        terminal = _replace(
            outcome, rows=tuple(row for row in outcome.rows if row['club_id'] == '631'),
            capture=_replace(page_2, url=ATTENDANCE_PAGE_2), **changes)
        return core.plan_for_outcome(terminal, captured_at=PAGE_2_CAPTURED, scope_values=scope_values)

    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    with pytest.raises(ValueError, match='page manifest'):
        terminal_only(pages=())
    with pytest.raises(CoreParseError, match='first_page'):
        terminal_only(pages=((ATTENDANCE_PAGE_2, page_2.capture_id, PAGE_2_CAPTURED,
                              page_2.body_hash, ''),))
    assert set(_stadiums(db)) == {'11', '631'}
    # A one-page edition carries its own one-page manifest.
    single = _attendance_capture('single_page.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    plan = _attendance_plan((single,))
    assert plan.pages == ((ATTENDANCE_START, single.capture_id, PAGE_1_CAPTURED, single.body_hash, ''),)
    # A page of another edition inside the manifest is not this scope's walk.
    elsewhere = ATTENDANCE_START.replace('/2025', '/2024')
    with pytest.raises(CoreParseError, match='page_scope_mismatch'):
        core.plan_for_outcome(
            _replace(outcome, pages=((elsewhere, page_1.capture_id, PAGE_1_CAPTURED,
                                      page_1.body_hash, outcome.pages[0][4]),
                                     *outcome.pages[1:])),
            captured_at=PAGE_2_CAPTURED, scope_values=scope_values)


def test_rowless_replacement_cannot_erase_a_capture_committed_with_rows():
    # Round 4 finding 3: a rowless plan returned "pending" before any capture
    # was compared, so it deleted a populated edition under the same capture.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    outcome, = sections.extract_attendance_observations(
        (page_1, page_2), _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
        edition_id='2025', extracted_at=LATER_EXTRACTED_AT, universe_version=UNIVERSE)
    rowless = _replace(outcome, status='observed_empty', status_reason='attendance:empty',
                       proof='attendance:empty', rows=())
    plan = core.plan_for_outcome(rowless, captured_at=PAGE_2_CAPTURED,
                                 scope_values={'competition_id': 'GB1', 'edition_id': '2025'})
    with pytest.raises(RuntimeError, match='replay differs'):
        _write(db, [plan], outcome.run_id, LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11', '631'}


def _moment(value) -> datetime:
    # The fake warehouse stores Trino's naive UTC wall time.
    return datetime.fromisoformat(str(value).replace(' ', 'T')).replace(tzinfo=timezone.utc)


def test_acknowledged_snapshot_with_a_rowless_page_replays_without_a_write():
    # Round 4 finding 5: the empty page left no committed row, so the replay
    # saw a capture the scope "did not hold" and replaced the edition again.
    rowless_1 = _page(ATTENDANCE_START, ROWLESS_PAGE_1, PAGE_1_CAPTURED, 'rowless page 1')
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    db = CoreDB()
    writer = CoreWriter(db)
    plans = [_attendance_plan((rowless_1, page_2))]

    def commit():
        return native_writer.write_core_bundle(
            lambda: db, writer, plans, run_id=plans[0].rows[0]['run_id'],
            universe_version=UNIVERSE, committed_at=EXTRACTED_AT)

    first = commit()
    assert first['verified'] is True and set(_stadiums(db)) == {'631'}
    writes, events = len(writer.writes), len(db.events)
    again = commit()
    assert again == first
    assert len(writer.writes) == writes
    assert not [sql for sql in db.events[events:] if sql.lstrip().startswith('DELETE')]
    assert set(_stadiums(db)) == {'631'}


def test_acknowledged_replay_cannot_resurrect_a_page_a_rowless_walk_retired():
    # Round 4 finding 1: both snapshots share the 12:30 maximum, and the
    # retired page left no row for the per-URL guard to read.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, NEWER_PAGE_CAPTURED)
    empty_1 = _page(ATTENDANCE_START, ROWLESS_PAGE_1, '2026-11-20T12:15:00.000000+00:00',
                    'empty 12:15')
    original = [_attendance_plan((page_1, page_2))]
    db = CoreDB()
    first = _bundle(db, original, EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11', '631'}
    later = _attendance_plan((empty_1, page_2), LATER_EXTRACTED_AT)
    assert later.captured_at == original[0].captured_at == NEWER_PAGE_CAPTURED
    assert _bundle(db, [later], LATER_EXTRACTED_AT)['verified'] is True
    assert set(_stadiums(db)) == {'631'}
    with pytest.raises(StaleTransfermarktWrite):
        _write(db, original, first['run_id'], EXTRACTED_AT)
    assert set(_stadiums(db)) == {'631'}


def test_snapshot_receipt_keeps_the_clock_of_the_capture_it_names():
    # Round 4 finding 1: a receipt paired the 12:00 capture id with the
    # 12:30 clock of a newer page that contributed no row.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    empty_2 = _page(ATTENDANCE_PAGE_2, ROWLESS_TERMINAL_PAGE_2, NEWER_PAGE_CAPTURED, 'empty 12:30')
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, empty_2), LATER_EXTRACTED_AT)], LATER_EXTRACTED_AT)
    receipt = db.sql.execute(
        'SELECT source_capture_id, captured_at FROM transfermarkt_core_receipts_v1').fetchone()
    assert receipt[0] == page_1.capture_id and _moment(receipt[1]) == _moment(PAGE_1_CAPTURED)
    # The scope is still acknowledged as fresh as its newest walked page.
    older_2 = _page(ATTENDANCE_PAGE_2, ROWLESS_TERMINAL_PAGE_2, PAGE_2_CAPTURED, 'empty 12:05')
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, [_attendance_plan((page_1, older_2), LATER_EXTRACTED_AT)], LATER_EXTRACTED_AT)


def _rowless_tail_snapshot(extracted_at=EXTRACTED_AT):
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    empty_2 = _page(ATTENDANCE_PAGE_2, ROWLESS_TERMINAL_PAGE_2, PAGE_2_CAPTURED, 'empty tail')
    return [_attendance_plan((page_1, empty_2), extracted_at)], page_1, empty_2


def _manifest(db):
    """The pages of the scope's newest walk: (url, capture id), by url."""
    evidence = _manifest_evidence(db)
    return sorted((url, item[0]) for url, item in evidence.items())


def test_recovery_after_a_crash_before_the_manifest_records_it(monkeypatch):
    # The manifest follows the snapshot it belongs to. A crash in between
    # leaves the intent pending; the replay rewrites the snapshot and records it.
    plans, page_1, empty_2 = _rowless_tail_snapshot()
    db = CoreDB()
    writer = CoreWriter(db)
    run_id = plans[0].rows[0]['run_id']
    original = native_writer._record_pages

    def crash(*args):
        raise RuntimeError('crash before the manifest')

    monkeypatch.setattr(native_writer, '_record_pages', crash)
    with pytest.raises(RuntimeError, match='crash before the manifest'):
        _write(db, plans, run_id, EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11'}  # the snapshot's rows are already committed
    assert native_writer.write_journal.pending_intents({'run_id': run_id})
    monkeypatch.setattr(native_writer, '_record_pages', original)
    receipts = native_writer.replay_pending_intents(lambda: db, writer)
    assert len(receipts) == 1 and receipts[0]['verified'] is True
    assert native_writer.write_journal.pending_intents({'run_id': run_id}) == []
    assert _manifest(db) == [(ATTENDANCE_START, page_1.capture_id), (ATTENDANCE_PAGE_2, empty_2.capture_id)]
    assert set(_stadiums(db)) == {'11'}


def test_snapshot_committed_without_its_manifest_is_recorded_on_replay():
    # A snapshot the warehouse holds without a manifest is written once more
    # and recorded; from then on its replay is a pure acknowledgement.
    plans, page_1, empty_2 = _rowless_tail_snapshot()
    db = CoreDB()
    writer = CoreWriter(db)
    run_id = plans[0].rows[0]['run_id']

    def commit():
        return native_writer.write_core_bundle(
            lambda: db, writer, plans, run_id=run_id, universe_version=UNIVERSE,
            committed_at=EXTRACTED_AT)

    commit()
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')
    assert commit()['verified'] is True
    assert _manifest(db) == [(ATTENDANCE_START, page_1.capture_id), (ATTENDANCE_PAGE_2, empty_2.capture_id)]
    writes, events = len(writer.writes), len(db.events)
    assert commit()['verified'] is True
    assert len(writer.writes) == writes
    assert not [sql for sql in db.events[events:] if sql.lstrip().startswith('DELETE')]


def test_wholly_rowless_snapshot_is_proven_by_its_manifest():
    # No row at all: the accepted pages and their clocks live in the manifest.
    # (The donor proves an empty edition only through a recognised layout;
    # this is the plan the writer then receives.)
    early = ('2026-11-20T11:00:00.000000+00:00', '2026-11-20T11:05:00.000000+00:00')
    late = (PAGE_1_CAPTURED, PAGE_2_CAPTURED)

    def proven_empty(clocks, extracted_at):
        # A capture is one response at one clock: each page of each snapshot
        # has its own capture identity.
        pages = (_page(ATTENDANCE_START, ROWLESS_PAGE_1, clocks[0], 'empty 1 at ' + clocks[0]),
                 _page(ATTENDANCE_PAGE_2, ROWLESS_TERMINAL_PAGE_2, clocks[1], 'empty 2 at ' + clocks[1]))
        outcome, = sections.extract_attendance_observations(
            pages, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
            edition_id='2025', extracted_at=extracted_at, universe_version=UNIVERSE)
        plan = core.plan_for_outcome(
            _replace(outcome, status='observed_empty', status_reason='attendance:proven',
                     proof='attendance:proven', rows=()),
            captured_at=clocks[1], scope_values={'competition_id': 'GB1', 'edition_id': '2025'})
        return plan, outcome.run_id

    older, older_run = proven_empty(early, EXTRACTED_AT)
    newer, newer_run = proven_empty(late, LATER_EXTRACTED_AT)
    db = CoreDB()
    _write(db, [older], older_run, EXTRACTED_AT)
    _write(db, [newer], newer_run, LATER_EXTRACTED_AT)
    accepted = [(ATTENDANCE_START, newer.pages[0][1]), (ATTENDANCE_PAGE_2, newer.pages[1][1])]
    assert _manifest(db) == accepted
    # The acknowledged older emptiness cannot be acknowledged over the newer one.
    with pytest.raises(StaleTransfermarktWrite):
        _write(db, [older], older_run, EXTRACTED_AT)
    assert _manifest(db) == accepted


def test_timestamp_spellings_cannot_take_another_captures_keys():
    # Round 4 finding 8: the engine reads '15:00:00Z' and '15:00:00+00:00'
    # as one TIMESTAMP, so a respelled key reached the ownership guard as a
    # different key and then merged over the committed owner's rows.
    first = _referee_page(REFEREE_DIRECTORY_URL, _referee_body([('101', 'Ada Example')]))
    db = CoreDB()
    _bundle(db, _referee_plans(_referee_directory((first,)), {first.capture_id: REFEREE_CAPTURED}),
            REFEREE_EXTRACTED_AT)
    other = _referee_at(REFEREE_DIRECTORY_URL, _referee_body(
        [('101', 'Ada Example')], matches_of={'101': '13'}), 'e' * 64, REFEREE_CAPTURED)
    outcome, = _referee_directory((other,))
    respelled = _replace(outcome, rows=tuple(
        {**row, 'observed_at': REFEREE_CAPTURED.replace('+00:00', 'Z')} for row in outcome.rows))
    with pytest.raises((ValueError, RuntimeError)):
        plan = core.plan_for_outcome(
            respelled, captured_at=REFEREE_CAPTURED,
            scope_values={'competition_id': 'XY1', 'edition_id': '2025'})
        _bundle(db, [plan], REFEREE_EXTRACTED_AT)
    rows = db.sql.execute(
        "SELECT value FROM transfermarkt_referee_aggregate_observations "
        "WHERE referee_id = '101' AND metric = 'matches_officiated'").fetchall()
    assert rows == [('14',)]


def test_one_run_validates_every_bundle_before_its_first_mutation():
    # Round 4 finding 9: two targets of one run claimed the same natural keys
    # (one capture clock); the second failed only after the first had
    # committed its rows and warehouse receipts.
    bodies = {
        f'{CLUB_VALUE_URL}/stichtag/{day}': (CLUB_VALUE_FIXTURES / f'vie1-{day}.html').read_bytes()
        for day in ('2013-04-15', '2024-06-01')}

    class _OneClock:
        """Both pages answer with one capture clock."""

        def __init__(self):
            self.calls = []

        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            self.calls.append(url)
            body = bodies[url]
            digest = hashlib.sha256(body).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
                raw_body_hash=digest, raw_fetched_at=PAGE_1_CAPTURED, cache_hit=False,
                status_code=200)

    plan = CoreScopePlan(
        league='VIE1', season='2526', scope_id='s', cycle_id='c',
        club_value_targets=(ClubValueTarget('VIE1', 'v-league-1', '2013-04-15'),
                            ClubValueTarget('VIE1', 'v-league-1', '2024-06-01')))
    fetcher = _OneClock()
    db = CoreDB()
    run = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT)
    with pytest.raises(ValueError, match='duplicate core natural key'):
        run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    # Nothing was written: neither a data table nor a receipt.
    assert db.sql.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name LIKE 'transfermarkt_%'"
    ).fetchone()[0] == 0
    assert fetcher.calls == list(bodies)


def test_plan_rows_carry_exactly_the_entity_columns():
    # Round 4 finding 12: a row that lost a business column used to be
    # written with a null over the committed value; only the physical
    # read-back noticed, after the replacement.
    pages = (_attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED),
             _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED))
    outcome, = sections.extract_attendance_observations(
        pages, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
        edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert all(row['capacity'] is not None for row in outcome.rows)
    stripped = _replace(outcome, rows=tuple(
        {column: value for column, value in row.items() if column != 'capacity'}
        for row in outcome.rows))
    assert 'row_column_mismatch' in core.build_core_dq([stripped])['errors']
    db = CoreDB()
    _bundle(db, [_attendance_plan(pages)], EXTRACTED_AT)
    with pytest.raises(CoreParseError, match='row_column_mismatch'):
        core.plan_for_outcome(stripped, captured_at=PAGE_2_CAPTURED,
                              scope_values={'competition_id': 'GB1', 'edition_id': '2025'})
    # A column the entity does not have is refused just the same.
    extra = _replace(outcome, rows=tuple({**row, 'donor_extra': 'x'} for row in outcome.rows))
    with pytest.raises(CoreParseError, match='row_column_mismatch'):
        core.plan_for_outcome(extra, captured_at=PAGE_2_CAPTURED,
                              scope_values={'competition_id': 'GB1', 'edition_id': '2025'})


def test_a_populated_page_verdict_cannot_be_an_absence():
    # Round 4 finding 14: a populated referee profile and its row were both
    # marked explicit_none and still passed DQ and write verification.
    profile, = sections.extract_referee_profile_observations(
        _referee_profile_capture(), _referee_scope(), referee_id='101',
        extracted_at=REFEREE_EXTRACTED_AT, universe_version=UNIVERSE)
    assert core.build_core_dq([profile])['passed'] is True
    # A populated page is an observed page, whatever verdict its rows claim.
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        _replace(profile, status='explicit_none',
                 rows=({**profile.rows[0], 'observation_status': 'explicit_none'},))
    for status in ('unknown', 'error', 'unavailable'):
        with pytest.raises(CoreParseError, match='status_rows_mismatch'):
            _replace(profile, status=status)


def test_club_value_rows_carry_an_amount_exactly_when_present():
    # Round 4 finding 14: rows marked explicit_none kept their amounts.
    outcomes = sections.extract_club_value_observations(
        _club_value_capture(), _club_value_scope(), competition_id='VIE1', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE)
    club = next(outcome for outcome in outcomes
                if all(row['value_amount'] is not None for row in outcome.rows))
    assert core.build_core_dq([club])['passed'] is True
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        _replace(club, status='explicit_none', status_reason='club_values:club_rows_explicit_none',
                 rows=tuple({**row, 'observation_status': 'explicit_none'} for row in club.rows))
    # Rows stay mutable after construction: DQ and planning recheck it.
    club.rows[0]['value_amount'] = None
    assert 'status_rows_mismatch' in core.build_core_dq([club])['errors']
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        core.plan_for_outcome(club, captured_at=CLUB_CAPTURED,
                              scope_values={'club_id': club.rows[0]['club_id']})
    context = _outcome_context('club_value_observations')
    for verdict, amount in (('observed_present', None), ('explicit_none', 1275000),
                            ('unknown', 1275000)):
        with pytest.raises(CoreParseError, match='status_rows_mismatch'):
            core.LaneOutcome('club_value_observations', 'observed_present',
                             'club_values:club_rows_present',
                             ({'observation_status': verdict, 'value_amount': amount},), **context)
    # An explicit absence without an amount stays valid.
    core.LaneOutcome('club_value_observations', 'explicit_none',
                     'club_values:club_rows_explicit_none',
                     ({'observation_status': 'explicit_none', 'value_amount': None},), **context)


def test_a_value_the_row_cannot_hold_is_the_sections_error_not_the_runs():
    # Round 4 finding 15: an oversized count raised out of the attendance
    # adapter, and the referee profile queued after it was never fetched.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    overflow = _page(ATTENDANCE_START, page_1.body.replace(b'60.704', b'9223372036854775808'),
                     NEWER_PAGE_CAPTURED, 'overflow')
    fetcher = _SectionFetch({
        ATTENDANCE_START: overflow.body, ATTENDANCE_PAGE_2: page_2.body,
        REFEREE_PROFILE_URL: (REFEREE_FIXTURES / 'profile.html').read_bytes()})
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c', edition_id='2025',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),),
        referee_profile_ids=('101',))
    db = CoreDB()
    report = CoreRun(fetcher.fetch, universe_version=UNIVERSE,
                     extracted_at=REFEREE_EXTRACTED_AT).run(
        plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert fetcher.calls == [ATTENDANCE_START, ATTENDANCE_PAGE_2, REFEREE_PROFILE_URL]
    assert [(item['entity'], item['status'], item['status_reason'], item['rows'])
            for item in report.outcomes] == [
        ('attendance_observations', 'error', 'attendance:row_value_exceeds_bigint', 0),
        ('referee_profiles', 'observed_present', 'referees:profile', 1)]
    assert [item['entity'] for item in report.receipts[0]['plans']] == ['referee_profiles']
    assert report.dq['passed'] is True


def test_club_value_competition_ids_keep_the_donors_grammar():
    # Round 4 finding 16: the donor accepts competition ids like U-21; the
    # run pushed the target through the league grammar of its own scope.
    body = (CLUB_VALUE_FIXTURES / 'vie1-current.html').read_bytes().replace(b'VIE1', b'U-21')
    url = CLUB_VALUE_URL.replace('/VIE1', '/U-21')
    fetcher = _SectionFetch({url: body})
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        club_value_targets=(ClubValueTarget('U-21', 'v-league-1'),))
    db = CoreDB()
    report = CoreRun(fetcher.fetch, universe_version=UNIVERSE,
                     extracted_at=EXTRACTED_AT).run(
        plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert [item['status'] for item in report.outcomes] == ['observed_present'] * 14
    assert report.dq['passed'] is True and report.receipts[0]['verified'] is True
    assert db.sql.execute(
        'SELECT DISTINCT competition_id FROM transfermarkt_club_value_observations').fetchall() == [
        ('U-21',)]


def test_referee_and_coach_sources_are_checked_by_their_full_origin():
    # Round 4 finding 17: the profile check compared only the hostname, so a
    # capture served from another port of it received a verified receipt.
    elsewhere = _replace(_referee_profile_capture(),
                         url=REFEREE_PROFILE_URL.replace('.com/', '.com:444/'))
    outcome, = sections.extract_referee_profile_observations(
        elsewhere, _referee_scope(), referee_id='101', extracted_at=REFEREE_EXTRACTED_AT,
        universe_version=UNIVERSE)
    assert (outcome.status, outcome.status_reason, outcome.rows) == (
        'error', 'referees:row_scope_mismatch', ())
    with pytest.raises(CoreParseError, match='row_scope_mismatch'):
        core._check_scope_against_capture('referee_profiles', {'referee_id': '101'}, elsewhere)
    # The coach achievement page follows the same origin rule.
    coach = _capture('https://www.transfermarkt.com:444/x/erfolge/trainer/7', b'<html></html>',
                     CLUB_CAPTURED, endpoint='coach_achievement')
    with pytest.raises(CoreParseError, match='row_scope_mismatch'):
        core._check_scope_against_capture(
            'achievement_observations', {'recipient_type': 'coach', 'recipient_id': '7'}, coach)


def test_referee_directory_urls_carry_no_query_and_validate_their_first_page():
    # Round 4 finding 17: the query of a directory URL was ignored, so a
    # terminal page offered as page 1 verified as a whole directory, and a
    # malformed first path raised out of the adapter.
    body = _referee_directory_capture().body
    terminal = _referee_page(REFEREE_DIRECTORY_URL + '?page=2', body)
    outcome = _directory_error((terminal,))
    assert outcome.status_reason == 'referees:directory_identity_invalid'
    malformed = _referee_page(REFEREE_DIRECTORY_URL + '/extra', body)
    outcome = _directory_error((malformed,))
    assert (outcome.status_reason, outcome.capture.capture_id) == (
        'referees:directory_identity_invalid', malformed.capture_id)
    with pytest.raises(CoreParseError, match='row_scope_mismatch'):
        core._check_scope_against_capture(
            'referee_aggregate_observations', {'competition_id': 'XY1', 'edition_id': '2025'},
            terminal)
    # The canonical directory URL of the scope is the one that is accepted.
    core._check_scope_against_capture(
        'referee_aggregate_observations', {'competition_id': 'XY1', 'edition_id': '2025'},
        _referee_page(REFEREE_DIRECTORY_URL, body))



def test_rumour_row_parse_failure_cites_the_page_that_failed():
    # Round 4 finding 18: a row of page 1 could not be parsed, and the error
    # outcome named page 2, the newest one, as its evidence.
    broken = _rumour_feed_html(1, 2, (1, 2)).replace(
        b'<a href="/p/profil/spieler/17">Player P</a>', b'Player P')
    page_1 = _rumour_capture(1, RUMOUR_PAGE_1_CAPTURED, body=broken)
    page_2 = _rumour_capture(2, RUMOUR_PAGE_2_CAPTURED)
    outcome, = sections.extract_rumour_observations(
        (page_1, page_2), _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE)
    assert (outcome.status, outcome.status_reason) == ('error', 'rumours:invalid_player')
    assert outcome.capture.capture_id == page_1.capture_id
    assert outcome.rows == ()


def test_empty_rumour_proof_ignores_a_marker_in_the_header():
    # Round 4 finding 19: the marker selector searched the whole table, so a
    # marked header cell over an empty body passed for the donor's marker row.
    header_marker = ('<table class="items"><thead><tr><td class="no-entries">Player</td>'
                     '<th>Interested club</th><th>Most recent source from</th>'
                     '<th>Assessment</th></tr></thead><tbody></tbody></table>')
    capture = _capture(RUMOUR_PAGE[1], f'<div id="yw1">{header_marker}{_rumour_pager(1, 1)}</div>'.encode(),
                       RUMOUR_PAGE_1_CAPTURED, endpoint='rumours')
    outcome, = sections.extract_rumour_observations(
        (capture,), _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE)
    assert (outcome.status, outcome.proof) == ('observed_empty', 'rumours:empty_table_body')
    # The donor's marker row keeps its own proof.
    marker = _capture(RUMOUR_PAGE[1], _no_entries_html(1, 1), RUMOUR_PAGE_1_CAPTURED,
                      endpoint='rumours')
    outcome, = sections.extract_rumour_observations(
        (marker,), _rumour_scope(), competition=_rumour_competition(),
        extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE)
    assert outcome.proof == 'rumours:no_entries_row'


def test_attendance_first_page_number_is_compared_numerically():
    # Round 4 finding 20: the donor reads ?page=01 as page 1; the adapter
    # compared the query as text and refused a donor-valid walk.
    start = ATTENDANCE_START + '?page=01'
    page_1 = _attendance_capture('page_1.html', start, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    outcome, = sections.extract_attendance_observations(
        (page_1, page_2), _attendance_scope(), start_url=start, competition_id='GB1',
        edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert (outcome.status, len(outcome.rows)) == ('observed_present', 2)
    plan = core.plan_for_outcome(outcome, captured_at=PAGE_2_CAPTURED,
                                 scope_values={'competition_id': 'GB1', 'edition_id': '2025'})
    assert plan.pages[0][0] == start
    # A later page is still not a start, however it is spelled.
    later = _replace(page_1, url=ATTENDANCE_START + '?page=02')
    outcome, = sections.extract_attendance_observations(
        (later, page_2), _attendance_scope(), start_url=later.url, competition_id='GB1',
        edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert (outcome.status, outcome.status_reason) == (
        'error', 'attendance:pagination_start_not_first_page')


# --------------------------------- review round-5 pins (findings 1-25)

def test_a_manifest_must_be_the_whole_walk_not_only_its_start():
    # Round 5 finding 3: page 1 refreshed, its row and evidence retained
    # alone, although the page's own HTML still links to page 2. One first
    # page proves the start of a walk, not its end.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    # A later capture of the same page has its own identity.
    refreshed = _page(ATTENDANCE_START, page_1.body, NEWER_PAGE_CAPTURED, 'refreshed 12:30')
    scope_values = {'competition_id': 'GB1', 'edition_id': '2025'}

    def extract(captures):
        outcome, = sections.extract_attendance_observations(
            captures, _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
            edition_id='2025', extracted_at=LATER_EXTRACTED_AT, universe_version=UNIVERSE)
        return outcome

    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    outcome = extract((refreshed, page_2))
    assert [page[0] for page in outcome.pages] == [ATTENDANCE_START, ATTENDANCE_PAGE_2]
    assert outcome.pages[0][4] == ATTENDANCE_PAGE_2 and outcome.pages[1][4] == ''
    with pytest.raises((ValueError, CoreParseError)):
        prefix = _replace(outcome, capture=refreshed,
                          rows=tuple(row for row in outcome.rows if row['club_id'] == '11'),
                          pages=outcome.pages[:1])
        _bundle(db, [core.plan_for_outcome(prefix, captured_at=NEWER_PAGE_CAPTURED,
                                           scope_values=scope_values)], LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11', '631'}
    # A page dropped from the middle or a reordered walk is not a walk either.
    for pages in (outcome.pages[::-1], outcome.pages[1:]):
        with pytest.raises((ValueError, CoreParseError)):
            core.plan_for_outcome(_replace(outcome, pages=pages),
                                  captured_at=NEWER_PAGE_CAPTURED, scope_values=scope_values)


def test_reordered_captures_of_one_walk_do_not_make_a_second_snapshot():
    # Round 5 finding 18: the adapter accepted either order of the same two
    # saved pages, hashed the manifest as supplied, and acknowledged two
    # different snapshots of one edition.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    forward = _attendance_plan((page_1, page_2))
    backward = _attendance_plan((page_2, page_1))
    assert [page[0] for page in backward.pages] == [ATTENDANCE_START, ATTENDANCE_PAGE_2]
    assert backward.plan_sha256() == forward.plan_sha256()
    db = CoreDB()
    _bundle(db, [forward], EXTRACTED_AT)
    again = _bundle(db, [backward], EXTRACTED_AT)
    assert again == _bundle(db, [forward], EXTRACTED_AT)
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone()[0] == 1


T1215 = '2026-11-20T12:15:00.000000+00:00'
T1235 = '2026-11-20T12:35:00.000000+00:00'
T1245 = '2026-11-20T12:45:00.000000+00:00'


def _manifest_evidence(db):
    """Newest accepted page evidence of the scope: URL -> (capture id, moment, body hash).

    Every acknowledgement logs its walk; a later log row of a page supersedes an earlier one.
    """
    newest = {}
    for url, capture_id, clock, body_hash, _acknowledged_at in db.sql.execute(
            'SELECT source_url, source_capture_id, captured_at, source_body_hash, acknowledged_at '
            'FROM transfermarkt_core_pages_v1 ORDER BY captured_at, acknowledged_at'):
        newest[url] = (capture_id, _moment(clock), body_hash)
    return newest


def _two_pages(first_clock=PAGE_1_CAPTURED, second_clock=PAGE_2_CAPTURED):
    return (_attendance_capture('page_1.html', ATTENDANCE_START, first_clock),
            _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, second_clock))


def test_an_interrupted_manifest_update_keeps_the_last_accepted_page(monkeypatch):
    # Round 5 finding 1: the manifest scope was deleted before its rows were
    # re-inserted; an interruption in between erased the accepted clock of a
    # rowless page and let the acknowledged older snapshot through.
    page_1, page_2 = _two_pages(second_clock=NEWER_PAGE_CAPTURED)
    original = [_attendance_plan((page_1, page_2))]
    db = CoreDB()
    writer = CoreWriter(db)
    first = _bundle(db, original, EXTRACTED_AT)
    empty_1 = _page(ATTENDANCE_START, ROWLESS_PAGE_1, T1215, 'empty 12:15')
    _bundle(db, [_attendance_plan((empty_1, page_2), LATER_EXTRACTED_AT)], LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'631'}
    assert _manifest_evidence(db)[ATTENDANCE_START][:2] == (empty_1.capture_id, _moment(T1215))
    # A later walk refreshes the empty page; its manifest update is interrupted.
    refreshed = _page(ATTENDANCE_START, ROWLESS_PAGE_1, T1235, 'empty 12:35')
    pending = [_attendance_plan((refreshed, page_2), LATER_EXTRACTED_AT)]
    real = native_writer._record_pages
    armed = {'on': True}

    def interrupted(*args):
        # The one statement that logs the later walk is lost. Reading the
        # log keeps working.
        if armed['on']:
            armed['on'] = False
            raise RuntimeError('interrupted manifest')
        return real(*args)

    monkeypatch.setattr(native_writer, '_record_pages', interrupted)
    with pytest.raises(RuntimeError, match='interrupted manifest'):
        _write(db, pending, pending[0].rows[0]['run_id'], LATER_EXTRACTED_AT)
    assert not armed['on'] and set(_stadiums(db)) == {'631'}
    # Until the interrupted bundle is replayed, its damage must not reopen
    # the acknowledged older snapshot.
    with pytest.raises(StaleTransfermarktWrite):
        _write(db, original, first['run_id'], EXTRACTED_AT)
    assert set(_stadiums(db)) == {'631'}
    receipts = native_writer.replay_pending_intents(lambda: db, writer)
    assert len(receipts) == 1 and receipts[0]['verified'] is True
    assert _manifest_evidence(db)[ATTENDANCE_START][:2] == (refreshed.capture_id, _moment(T1235))
    with pytest.raises(StaleTransfermarktWrite):
        _write(db, original, first['run_id'], EXTRACTED_AT)
    assert set(_stadiums(db)) == {'631'}


def test_a_page_that_left_the_walk_keeps_its_accepted_clock():
    # Round 5 finding 2: a shorter walk replaced the whole manifest scope, and
    # the page that had left it came back later with an older cached capture.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    empty_2 = _page(ATTENDANCE_PAGE_2, ROWLESS_TERMINAL_PAGE_2, NEWER_PAGE_CAPTURED, 'empty 12:30')
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, empty_2))], EXTRACTED_AT)
    # The edition later fits on one page: page 2 is no longer walked.
    single = _page(ATTENDANCE_START, (ATTENDANCE_FIXTURES / 'single_page.html').read_bytes(),
                   T1235, 'single 12:35')
    assert _bundle(db, [_attendance_plan((single,), LATER_EXTRACTED_AT)], LATER_EXTRACTED_AT)['verified'] is True
    assert _manifest_evidence(db) == {
        ATTENDANCE_START: (single.capture_id, _moment(T1235), single.body_hash),
        ATTENDANCE_PAGE_2: (empty_2.capture_id, _moment(NEWER_PAGE_CAPTURED), empty_2.body_hash)}
    # A later page 1 links to a cached page 2 older than the accepted emptiness.
    late_1 = _page(ATTENDANCE_START, page_1.body, T1245, 'late 12:45')
    cached_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, [_attendance_plan((late_1, cached_2), LATER_EXTRACTED_AT)], LATER_EXTRACTED_AT)
    # A genuinely newer page 2 joins the walk again.
    fresh_2 = _page(ATTENDANCE_PAGE_2, cached_2.body, T1245, 'fresh 12:45')
    assert _bundle(db, [_attendance_plan((late_1, fresh_2), LATER_EXTRACTED_AT)],
                   LATER_EXTRACTED_AT)['verified'] is True


def test_page_evidence_cannot_change_for_a_capture_the_scope_accepted():
    # Round 5 finding 6: the committed manifest was compared without its
    # body hashes, so a rowless capture replayed with another body was
    # acknowledged a second time over the warehouse's original evidence.
    rowless_1 = _page(ATTENDANCE_START, ROWLESS_PAGE_1, PAGE_1_CAPTURED, 'rowless page 1')
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    db = CoreDB()
    plans = [_attendance_plan((rowless_1, page_2))]
    _bundle(db, plans, EXTRACTED_AT)
    plan = plans[0]
    first = plan.pages[0]
    rehashed = _replace(plan, pages=((first[0], first[1], first[2], 'e' * 64, first[4]), plan.pages[1]))
    with pytest.raises(RuntimeError, match='replay differs'):
        _write(db, [rehashed], plan.rows[0]['run_id'], LATER_EXTRACTED_AT)
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone()[0] == 1
    assert _manifest_evidence(db)[ATTENDANCE_START][2] == rowless_1.body_hash


def test_a_duplicated_manifest_row_is_a_corrupt_manifest():
    # Round 5 finding 6: two accepted rows for one page is not a manifest.
    rowless_1 = _page(ATTENDANCE_START, ROWLESS_PAGE_1, PAGE_1_CAPTURED, 'rowless page 1')
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    db = CoreDB()
    plans = [_attendance_plan((rowless_1, page_2))]
    _bundle(db, plans, EXTRACTED_AT)
    db.sql.execute(
        'INSERT INTO transfermarkt_core_pages_v1 SELECT * FROM transfermarkt_core_pages_v1 '
        'WHERE source_url = ?', (ATTENDANCE_START,))
    with pytest.raises(RuntimeError, match='manifest'):
        _bundle(db, plans, LATER_EXTRACTED_AT)
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone()[0] == 1


def test_a_snapshot_whose_manifest_was_not_persisted_is_not_acknowledged(monkeypatch):
    # Round 5 finding 6: the manifest was written but never read back.
    rowless_1 = _page(ATTENDANCE_START, ROWLESS_PAGE_1, PAGE_1_CAPTURED, 'rowless page 1')
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    db = CoreDB()
    plans = [_attendance_plan((rowless_1, page_2))]
    monkeypatch.setattr(native_writer, '_record_pages', lambda *args: None)
    with pytest.raises(RuntimeError, match='manifest'):
        _bundle(db, plans, EXTRACTED_AT)
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone()[0] == 0


def test_a_capture_committed_as_rowless_cannot_acquire_rows():
    # Round 5 finding 4: a row appeared under a capture the scope had
    # accepted as a page without rows, carrying that page's exact evidence.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    empty_2 = _page(ATTENDANCE_PAGE_2, ROWLESS_TERMINAL_PAGE_2, PAGE_2_CAPTURED, 'empty page 2')
    db = CoreDB()
    plans = [_attendance_plan((page_1, empty_2))]
    _bundle(db, plans, EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11'}
    plan = plans[0]
    page_1_again, populated_2 = _two_pages()
    chelsea = next(row for row in _attendance_plan((page_1_again, populated_2)).rows
                   if row['club_id'] == '631')
    _url, capture_id, _clock, body_hash, _next = plan.pages[1]
    acquired = {**chelsea, 'raw_capture_id': capture_id, 'source_capture_id': capture_id,
                'source_body_hash': body_hash}
    acquired_plan = _replace(plan, rows=plan.rows + (acquired,), capture_id=capture_id, evidence=())
    with pytest.raises(RuntimeError, match='replay differs'):
        _write(db, [acquired_plan], plan.rows[0]['run_id'], LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11'}


def test_a_rowless_snapshot_names_its_newest_page():
    # Round 5 finding 13: a wholly rowless snapshot named its first page
    # while its receipt stored the clock of the last one.
    empty_1 = _page(ATTENDANCE_START, ROWLESS_PAGE_1, PAGE_1_CAPTURED, 'empty 12:00')
    empty_2 = _page(ATTENDANCE_PAGE_2, ROWLESS_TERMINAL_PAGE_2, PAGE_2_CAPTURED, 'empty 12:05')
    outcome, = sections.extract_attendance_observations(
        (empty_1, empty_2), _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
        edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    proven = _replace(outcome, status='observed_empty', status_reason='attendance:proven',
                      proof='attendance:proven', rows=())
    scope_values = {'competition_id': 'GB1', 'edition_id': '2025'}
    with pytest.raises((ValueError, CoreParseError)):
        core.plan_for_outcome(_replace(proven, capture=empty_1), captured_at=PAGE_2_CAPTURED,
                              scope_values=scope_values)
    plan = core.plan_for_outcome(proven, captured_at=PAGE_2_CAPTURED, scope_values=scope_values)
    assert plan.capture_id == empty_2.capture_id
    db = CoreDB()
    _write(db, [plan], proven.run_id, EXTRACTED_AT)
    receipt = db.sql.execute(
        'SELECT source_capture_id, captured_at FROM transfermarkt_core_receipts_v1').fetchone()
    assert (receipt[0], _moment(receipt[1])) == (empty_2.capture_id, _moment(PAGE_2_CAPTURED))


def test_a_repair_of_an_acknowledged_snapshot_leaves_a_recoverable_intent(monkeypatch):
    # Round 5 finding 7: an acknowledged snapshot that has to be written
    # again was mutated without an intent, so an interrupted repair had
    # nothing left to replay.
    plans, page_1, empty_2 = _rowless_tail_snapshot()
    db = CoreDB()
    writer = CoreWriter(db)
    run_id = plans[0].rows[0]['run_id']

    def commit():
        return native_writer.write_core_bundle(
            lambda: db, writer, plans, run_id=run_id, universe_version=UNIVERSE,
            committed_at=EXTRACTED_AT)

    assert commit()['verified'] is True
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')

    def crash(*args):
        raise RuntimeError('crash during repair')

    real = native_writer._record_pages
    monkeypatch.setattr(native_writer, '_record_pages', crash)
    with pytest.raises(RuntimeError, match='crash during repair'):
        commit()
    # The repair is journalled before it mutates, like any other write.
    assert native_writer.write_journal.pending_intents({'run_id': run_id})
    monkeypatch.setattr(native_writer, '_record_pages', real)
    receipts = native_writer.replay_pending_intents(lambda: db, writer)
    assert len(receipts) == 1 and receipts[0]['verified'] is True
    assert native_writer.write_journal.pending_intents({'run_id': run_id}) == []
    assert _manifest(db) == [(ATTENDANCE_START, page_1.capture_id), (ATTENDANCE_PAGE_2, empty_2.capture_id)]
    assert set(_stadiums(db)) == {'11'}


def test_a_timestamp_that_does_not_parse_is_refused_before_the_bundle():
    # Round 5 finding 15: the canonical check compared a cell with itself
    # whenever it could not be parsed, so any text passed for a clock.
    first = _referee_page(REFEREE_DIRECTORY_URL, _referee_body([('101', 'Ada Example')]))
    outcome, = _referee_directory((first,))
    for column in ('extracted_at', 'captured_at', 'observed_at'):
        broken = _replace(outcome, rows=tuple({**row, column: 'yesterday'} for row in outcome.rows))
        with pytest.raises((ValueError, CoreParseError)):
            core.plan_for_outcome(broken, captured_at=REFEREE_CAPTURED,
                                  scope_values={'competition_id': 'XY1', 'edition_id': '2025'})
    with pytest.raises(ValueError, match='timestamp'):
        _replace(_attendance_plan(_two_pages()), captured_at='later that day')


def test_row_values_match_their_declared_column_types():
    # Round 5 finding 16: two capacities given as text were one consistent
    # type for their column; the replacement committed the text over the
    # committed numbers and only the read-back failed.
    page_1, page_2 = _two_pages()
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    plan = _attendance_plan((page_1, page_2), LATER_EXTRACTED_AT)
    with pytest.raises(ValueError, match='column type'):
        _replace(plan, rows=tuple({**row, 'capacity': str(row['capacity'])} for row in plan.rows))
    with pytest.raises(ValueError, match='column type'):
        _replace(plan, rows=tuple({**row, 'club_id': int(row['club_id'])} for row in plan.rows))
    with pytest.raises(ValueError, match='column type'):
        _replace(plan, rows=tuple({**row, 'final_season': 'yes'} for row in plan.rows))
    assert set(_stadiums(db)) == {'11', '631'}


def test_rows_of_one_capture_share_their_whole_lineage():
    # Round 5 finding 12: a row could name another raw capture, another
    # endpoint or another scope than its receipt, and rows of one capture
    # could carry different clocks.
    profile, = sections.extract_referee_profile_observations(
        _referee_profile_capture(), _referee_scope(), referee_id='101',
        extracted_at=REFEREE_EXTRACTED_AT, universe_version=UNIVERSE)
    row = profile.rows[0]
    for change in ({'raw_capture_id': 'f' * 64}, {'source_endpoint': 'rumours'},
                   {'scope_id': 'another-scope'}, {'cycle_id': 'another-cycle'}):
        with pytest.raises((ValueError, CoreParseError)):
            core.plan_for_outcome(_replace(profile, rows=({**row, **change},)),
                                  captured_at=REFEREE_CAPTURED, scope_values={'referee_id': '101'})
    achievements, = sections.extract_achievement_observations(
        _achievement_capture(ACHIEVEMENTS_PACKET), _achievement_scope(), recipient_type='player',
        recipient_id='7', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert len(achievements.rows) == 2
    first, second = achievements.rows
    for change in ({'captured_at': '2026-11-20T09:00:00.000000+00:00'},
                   {'source_url': ACHIEVEMENTS_URL + '/other'}, {'run_id': 'another-run'},
                   {'universe_version': 'another-universe'}):
        with pytest.raises((ValueError, CoreParseError)):
            core.plan_for_outcome(
                _replace(achievements, rows=({**first, **change}, second)),
                captured_at=CLUB_CAPTURED,
                scope_values={'recipient_type': 'player', 'recipient_id': '7'})


def test_a_contradictory_empty_merge_is_refused_before_its_siblings_mutate():
    # Round 5 finding 17: an empty merge plan whose capture is committed
    # with rows was only rejected by its read-back, after the replacement
    # sharing its bundle had already been written.
    achievements, = sections.extract_achievement_observations(
        _achievement_capture(ACHIEVEMENTS_PACKET), _achievement_scope(), recipient_type='player',
        recipient_id='7', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    populated = core.plan_for_outcome(achievements, captured_at=CLUB_CAPTURED,
                                      scope_values={'recipient_type': 'player', 'recipient_id': '7'})
    db = CoreDB()
    _write(db, [populated], achievements.run_id, EXTRACTED_AT)
    page_1, page_2 = _two_pages()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    newer_1 = _page(ATTENDANCE_START, page_1.body, NEWER_PAGE_CAPTURED, 'newer page 1')
    replacement = _attendance_plan((newer_1, page_2), LATER_EXTRACTED_AT)
    empty = _replace(achievements, status='observed_empty', status_reason='achievements:empty_collection',
                     proof='achievements:empty_collection:player', rows=())
    before = dict(db.sql.execute(
        'SELECT club_id, captured_at FROM transfermarkt_attendance_observations').fetchall())
    with pytest.raises(RuntimeError, match='replay differs'):
        _write(db, [replacement, core.plan_for_outcome(
            empty, captured_at=CLUB_CAPTURED,
            scope_values={'recipient_type': 'player', 'recipient_id': '7'})],
            achievements.run_id, LATER_EXTRACTED_AT)
    assert dict(db.sql.execute(
        'SELECT club_id, captured_at FROM transfermarkt_attendance_observations').fetchall()) == before


def test_every_source_is_checked_by_its_complete_origin_and_route():
    # Round 5 finding 22: achievement sources were compared by hostname only
    # and their query and fragment were never read, so another port of the
    # API host and a coach page with a query were verified.
    body = json.dumps(ACHIEVEMENTS_PACKET).encode('utf-8')
    digest = hashlib.sha256(body).hexdigest()
    for url in ('https://tmapi.transfermarkt.technology:444/player/7/achievement',
                'https://tmapi.transfermarkt.technology/player/7/achievement?x=1',
                'https://tmapi.transfermarkt.technology/player/7/achievement#more',
                'https://user@tmapi.transfermarkt.technology/player/7/achievement'):
        capture = SourceCapture(url=url, endpoint='player_achievement', body=body,
                                capture_id=digest, body_hash=digest, captured_at=CLUB_CAPTURED)
        with pytest.raises(CoreParseError, match='row_scope_mismatch'):
            sections.extract_achievement_observations(
                capture, _achievement_scope(), recipient_type='player', recipient_id='7',
                extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    coach = {'recipient_type': 'coach', 'recipient_id': '7'}
    page = _capture('https://www.transfermarkt.com/x/erfolge/trainer/7', b'<html></html>',
                    CLUB_CAPTURED, endpoint='coach_achievement')
    for url in ('https://www.transfermarkt.com:444/x/erfolge/trainer/7',
                'https://www.transfermarkt.com/x/erfolge/trainer/7?page=2#more',
                'https://www.transfermarkt.com/x/erfolge/trainer/7?page=2',
                'https://www.transfermarkt.com/x/erfolge/trainer/7#more'):
        with pytest.raises(CoreParseError, match='row_scope_mismatch'):
            core._check_scope_against_capture('achievement_observations', coach, _replace(page, url=url))
    core._check_scope_against_capture('achievement_observations', coach, page)
    with pytest.raises(CoreParseError, match='row_scope_mismatch'):
        core._check_scope_against_capture(
            'player_representation_observations', {'player_id': '7'},
            _replace(_representation_capture(), url=REPRESENTATION_URL.replace('.technology/', '.technology:444/')))
    core._check_scope_against_capture(
        'player_representation_observations', {'player_id': '7'}, _representation_capture())
    # A stage-B lane keeps the same rule for its own origin.
    from tests.unit.scrapers.test_transfermarkt_core import _load
    squad = _load('squad')
    scope_values = {'league': 'GB1', 'season': '2627', 'club_id': squad.url.split('/verein/')[1].split('/')[0]}
    core._check_scope_against_capture('squad_memberships', scope_values, squad)
    for url in (squad.url.replace('.com/', '.com:444/'), squad.url + '?page=2', squad.url + '#x'):
        with pytest.raises(CoreParseError, match='row_scope_mismatch'):
            core._check_scope_against_capture('squad_memberships', scope_values, _replace(squad, url=url))


def test_a_referee_directory_cannot_bind_two_responses_to_one_capture():
    # Round 5 finding 20: the capture-conflict check covered attendance and
    # rumours only, so two directory pages under one capture id verified
    # rows with two body hashes under that id.
    page_1 = _referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body(),
                           next_url=REFEREE_DIRECTORY_PAGE_2)
    page_2 = _referee_page(REFEREE_DIRECTORY_PAGE_2, _referee_second_page_body())
    with pytest.raises(CoreParseError, match='referees:capture_conflict'):
        _referee_directory((page_1, _replace(page_2, capture_id=page_1.capture_id)))


def test_duplicate_targets_of_one_run_are_refused_before_any_write():
    # Round 5 finding 23: one empty packet requested twice planned the same
    # rowless merge twice. Empty plans have neither rows nor a replacement
    # scope, so the run-level check did not compare them.
    from scrapers.transfermarkt.orchestrator import AchievementHolderTarget
    replay = _JsonFetch(ACHIEVEMENTS_URL, ACHIEVEMENTS_EMPTY)
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        achievement_holders=(AchievementHolderTarget('player', '7'), AchievementHolderTarget('player', '7')))
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader, universe_version=UNIVERSE,
                  extracted_at=EXTRACTED_AT)
    db = CoreDB()
    with pytest.raises(ValueError, match='duplicate'):
        run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert not db.sql.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name LIKE 'transfermarkt_%'"
    ).fetchall()


def test_a_parse_error_cites_the_page_that_failed():
    # Round 5 finding 24: a donor error over the whole walk was backed by the
    # newest page, which was a valid one.
    page_1 = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    page_2 = _attendance_capture('page_2.html', ATTENDANCE_PAGE_2, PAGE_2_CAPTURED)
    broken_1 = _page(ATTENDANCE_START, page_1.body.replace(b'60.704', b'not-a-number'),
                     PAGE_1_CAPTURED, 'broken page 1')
    assert broken_1.body != page_1.body
    outcome, = sections.extract_attendance_observations(
        (broken_1, page_2), _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
        edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert (outcome.status, outcome.rows) == ('error', ())
    assert outcome.capture.capture_id == broken_1.capture_id
    broken_2 = _page(ATTENDANCE_PAGE_2, page_2.body.replace(b'40,173', b'not-a-number'),
                     PAGE_2_CAPTURED, 'broken page 2')
    assert broken_2.body != page_2.body
    page_1_again = _attendance_capture('page_1.html', ATTENDANCE_START, PAGE_1_CAPTURED)
    outcome, = sections.extract_attendance_observations(
        (page_1_again, broken_2), _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
        edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert (outcome.status, outcome.capture.capture_id) == ('error', broken_2.capture_id)
    # The same for a directory page the donor cannot read.
    first = _referee_page(REFEREE_DIRECTORY_URL, b'<html><body>no table here</body></html>',
                          next_url=REFEREE_DIRECTORY_PAGE_2)
    second = _referee_page(REFEREE_DIRECTORY_PAGE_2, _referee_second_page_body())
    outcome = _directory_error((first, second))
    assert outcome.capture.capture_id == first.capture_id


def test_a_referee_profile_target_is_canonicalised_once():
    # Round 5 finding 25: the request URL normalised '0101' to referee 101
    # while the requested scope kept '0101'.
    body = (REFEREE_FIXTURES / 'profile.html').read_bytes()

    class _Fetch:
        def fetch(self, url, *, as_json, label, context=None, validator=None, **kwargs):
            assert url == REFEREE_PROFILE_URL
            digest = hashlib.sha256(body).hexdigest()
            return FetchOutcome(
                status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
                raw_body_hash=digest, raw_fetched_at=REFEREE_CAPTURED, cache_hit=False,
                status_code=200)

    plan = CoreScopePlan(league='XY1', season='2526', scope_id='s', cycle_id='c',
                         referee_profile_ids=('0101',))
    db = CoreDB()
    report = CoreRun(_Fetch().fetch, universe_version=UNIVERSE,
                     extracted_at=REFEREE_EXTRACTED_AT).run(
        plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert [(item['entity'], item['status']) for item in report.outcomes] == [
        ('referee_profiles', 'observed_present')]
    assert report.receipts[0]['verified'] is True
    assert db.sql.execute('SELECT referee_id FROM transfermarkt_referee_profiles').fetchall() == [('101',)]


def test_a_value_a_row_cannot_hold_fails_only_its_own_target_in_every_lane():
    # Round 5 finding 21: rendering sat outside the source-error handler of
    # every lane but attendance, so one oversized source amount aborted the
    # run and the target queued after it was never fetched.
    body = (CLUB_VALUE_FIXTURES / 'vie1-current.html').read_bytes()
    chart_value = b"'y':1275000"
    assert chart_value in body
    fetcher = _SectionFetch({
        CLUB_VALUE_URL: body.replace(chart_value, b"'y':9223372036854775808", 1),
        REFEREE_PROFILE_URL: (REFEREE_FIXTURES / 'profile.html').read_bytes()})
    plan = CoreScopePlan(
        league='VIE1', season='2526', scope_id='s', cycle_id='c',
        club_value_targets=(ClubValueTarget('VIE1', 'v-league-1'),),
        referee_profile_ids=('101',))
    db = CoreDB()
    report = CoreRun(fetcher.fetch, universe_version=UNIVERSE,
                     extracted_at=REFEREE_EXTRACTED_AT).run(
        plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert fetcher.calls == [CLUB_VALUE_URL, REFEREE_PROFILE_URL]
    assert [(item['entity'], item['status'], item['status_reason'], item['rows'])
            for item in report.outcomes] == [
        ('club_value_observations', 'error', 'club_values:row_value_exceeds_bigint', 0),
        ('referee_profiles', 'observed_present', 'referees:profile', 1)]
    assert report.dq['passed'] is True


@pytest.mark.parametrize('lane', [
    'club_values', 'referee_directory', 'referee_profile', 'achievement', 'representation',
    'rumours'])
def test_an_unrenderable_row_is_its_sections_error(monkeypatch, lane):
    # Round 5 finding 21: rendering sat outside the handler of every adapter
    # but attendance. A row the core cannot render (forced here; the donors
    # type most of their values themselves) is the section's error outcome,
    # never an exception out of the adapter.
    real = core._render
    entity = {
        'club_values': 'club_value_observations',
        'referee_directory': 'referee_aggregate_observations',
        'referee_profile': 'referee_profiles',
        'achievement': 'achievement_observations',
        'representation': 'player_representation_observations',
        'rumours': 'rumour_observations',
    }[lane]

    def refusing(target, *args, **kwargs):
        if target == entity:
            raise core.CoreParseError('row_value_exceeds_bigint')
        return real(target, *args, **kwargs)

    monkeypatch.setattr(core, '_render', refusing)
    directory = (_referee_page(REFEREE_DIRECTORY_URL, _referee_first_page_body(),
                               next_url=REFEREE_DIRECTORY_PAGE_2),
                 _referee_page(REFEREE_DIRECTORY_PAGE_2, _referee_second_page_body()))
    feed = (_rumour_capture(1, RUMOUR_PAGE_1_CAPTURED), _rumour_capture(2, RUMOUR_PAGE_2_CAPTURED))
    outcomes = {
        'club_values': lambda: sections.extract_club_value_observations(
            _club_value_capture(), _club_value_scope(), competition_id='VIE1',
            extracted_at=EXTRACTED_AT, universe_version=UNIVERSE),
        'referee_directory': lambda: _referee_directory(directory),
        'referee_profile': lambda: sections.extract_referee_profile_observations(
            _referee_profile_capture(), _referee_scope(), referee_id='101',
            extracted_at=REFEREE_EXTRACTED_AT, universe_version=UNIVERSE),
        'achievement': lambda: sections.extract_achievement_observations(
            _achievement_capture(ACHIEVEMENTS_PACKET), _achievement_scope(),
            recipient_type='player', recipient_id='7', extracted_at=EXTRACTED_AT,
            universe_version=UNIVERSE),
        'representation': lambda: sections.extract_representation_observations(
            _representation_capture(), _representation_scope(), expected_ids=('7', '9'),
            extracted_at=EXTRACTED_AT, universe_version=UNIVERSE),
        'rumours': lambda: sections.extract_rumour_observations(
            feed, _rumour_scope(), competition=_rumour_competition(),
            extracted_at=RUMOUR_EXTRACTED_AT, universe_version=UNIVERSE),
    }[lane]()
    assert outcomes
    for outcome in outcomes:
        assert (outcome.status, outcome.rows) == ('error', ())
        assert outcome.status_reason.endswith(':row_value_exceeds_bigint')
        assert outcome.capture is not None
    assert core.build_core_dq(outcomes)['passed'] is True


def test_a_rows_verdict_must_agree_with_the_facts_the_row_carries():
    # Round 5 finding 19: a row kept its donor's facts (a crowd figure, an
    # agency, a metric value) under a verdict that denies them, and DQ and
    # planning certified it.
    page_1, page_2 = _two_pages()
    outcome, = sections.extract_attendance_observations(
        (page_1, page_2), _attendance_scope(), start_url=ATTENDANCE_START, competition_id='GB1',
        edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert all(row['spectators_total'] for row in outcome.rows)
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        _replace(outcome, status='explicit_none',
                 rows=tuple({**row, 'observation_status': 'explicit_none'} for row in outcome.rows))
    for row in outcome.rows:
        row['observation_status'] = 'explicit_none'
    assert 'status_rows_mismatch' in core.build_core_dq([outcome])['errors']
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        core.plan_for_outcome(outcome, captured_at=PAGE_2_CAPTURED,
                              scope_values={'competition_id': 'GB1', 'edition_id': '2025'})

    assigned = next(item for item in sections.extract_representation_observations(
        _representation_capture(), _representation_scope(), expected_ids=('7', '9'),
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE) if item.status == 'observed_present')
    assert assigned.rows[0]['representative_key'] == 'agency:55'
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        _replace(assigned, status='explicit_none',
                 rows=({**assigned.rows[0], 'observation_status': 'explicit_none'},))
    assigned.rows[0]['observation_status'] = 'explicit_none'
    assert 'status_rows_mismatch' in core.build_core_dq([assigned])['errors']

    first = _referee_page(REFEREE_DIRECTORY_URL, _referee_body([('101', 'Ada Example')]))
    directory, = _referee_directory((first,))
    published = [row for row in directory.rows if row['observation_status'] == 'observed_present']
    assert any(row['metric'] == 'matches_officiated' and row['value'] == '14' for row in published)
    absent = tuple({**row, 'observation_status': 'explicit_none'} for row in directory.rows)
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        _replace(directory, rows=absent)
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        _replace(directory, rows=tuple(
            {**row, 'value': None} if row['observation_status'] == 'observed_present' else row
            for row in directory.rows))

    values = sections.extract_club_value_observations(
        _club_value_capture(), _club_value_scope(), competition_id='VIE1',
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    club = next(item for item in values if all(row['value_amount'] is not None for row in item.rows))
    club.rows[0]['value_amount'] = -1
    assert 'status_rows_mismatch' in core.build_core_dq([club])['errors']
    with pytest.raises(CoreParseError, match='status_rows_mismatch'):
        core.plan_for_outcome(club, captured_at=CLUB_CAPTURED,
                              scope_values={'club_id': club.rows[0]['club_id']})


# --------------------------------- review round-6 pins


def _attendance_outcome(captures, start_url=ATTENDANCE_START):
    outcome, = sections.extract_attendance_observations(
        captures, _attendance_scope(), start_url=start_url, competition_id='GB1',
        edition_id='2025', extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    return outcome


def test_finding_14_attendance_manifest_is_numbered_in_walk_order():
    # Round 6 finding 14: even a self-consistent chain must visit page 1 before page 2.
    outcome = _attendance_outcome(_two_pages())
    values = {'competition_id': 'GB1', 'edition_id': '2025'}
    plan = core.plan_for_outcome(outcome, captured_at=PAGE_2_CAPTURED, scope_values=values)
    assert plan.pages == outcome.pages
    second, first = outcome.pages[::-1]
    reversed_walk = (second[:4] + (first[0],), first[:4] + ('',))
    broken = _replace(outcome, pages=reversed_walk)
    with pytest.raises(CoreParseError, match='page_manifest_does_not_start_on_the_first_page'):
        core.plan_for_outcome(broken, captured_at=PAGE_2_CAPTURED, scope_values=values)


def test_finding_20_duplicate_attendance_url_is_refused_in_either_order():
    # Round 6 finding 20: a populated and a rowless capture cannot both own page 1.
    first, second = _two_pages()
    rowless = _capture(ATTENDANCE_START, ROWLESS_PAGE_1, PAGE_1_CAPTURED)
    assert first.capture_id != rowless.capture_id
    for captures in ((first, rowless, second), (rowless, first, second)):
        outcome = _attendance_outcome(captures)
        assert (outcome.status, outcome.status_reason, outcome.rows) == (
            'error', 'attendance:pagination_captures_differ', ())


def test_finding_22b_club_values_bind_every_row_to_the_url_competition():
    # Round 6 finding 22: the club scope alone cannot certify the page's competition.
    capture = _club_value_capture()
    clubs = sections.extract_club_value_observations(
        capture, _club_value_scope(), competition_id='VIE1', extracted_at=EXTRACTED_AT,
        universe_version=UNIVERSE)
    club = next(item for item in clubs if item.status == 'observed_present')
    values = {'club_id': club.rows[0]['club_id']}
    assert core.plan_for_outcome(club, captured_at=capture.captured_at, scope_values=values)
    wrong = _replace(capture, url=capture.url.replace('/VIE1', '/GB1'))
    with pytest.raises(CoreParseError, match='row_scope_mismatch'):
        core.plan_for_outcome(_replace(club, capture=wrong), captured_at=capture.captured_at,
                              scope_values=values)


@pytest.mark.parametrize('extra', ['&other=yes', '&ids[]=', '&ids[]=09'])
def test_finding_22c_representation_query_validates_every_key_and_id(extra):
    # Round 6 finding 22: a valid player in the query cannot hide an invalid parameter.
    capture = _representation_capture()
    wrong = _replace(capture, url=capture.url + extra)
    args = dict(expected_ids=('7', '9'), extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    error, = sections.extract_representation_observations(wrong, _representation_scope(), **args)
    assert (error.status, error.status_reason, error.rows) == (
        'error', 'representation:requested_ids_differ', ())
    assigned, _ = sections.extract_representation_observations(capture, _representation_scope(), **args)
    with pytest.raises(CoreParseError, match='row_scope_mismatch'):
        core.plan_for_outcome(_replace(assigned, capture=wrong), captured_at=capture.captured_at,
                              scope_values={'player_id': '7'})


def test_finding_23_http_attendance_continuation_is_never_fetched():
    # Round 6 finding 23: an HTTP next link fails attendance and the next target still verifies.
    body = ROWLESS_PAGE_1.replace(b'href="/', b'href="http://www.transfermarkt.com/')
    assert ATTENDANCE_PAGE_2.replace('https:', 'http:').encode() in body
    fetcher = _SectionFetch({ATTENDANCE_START: body, REFEREE_PROFILE_URL: _referee_profile_capture().body})
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),),
        referee_profile_ids=('101',))
    db = CoreDB()
    report = CoreRun(fetcher.fetch, universe_version=UNIVERSE, extracted_at=EXTRACTED_AT).run(
        plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert fetcher.calls == [ATTENDANCE_START, REFEREE_PROFILE_URL]
    attendance, profile = report.outcomes
    assert (attendance['status'], attendance['status_reason'], attendance['rows']) == (
        'error', 'attendance:pagination_identity_invalid', 0)
    assert (profile['entity'], profile['status']) == ('referee_profiles', 'observed_present')
    receipt, = report.receipts
    assert receipt['verified'] is True


def test_finding_23_saved_attendance_page_with_a_fragment_cannot_write():
    # Round 6 finding 23: canonicalisation cannot strip a fragment from saved evidence.
    first, second = _two_pages()
    outcome = _attendance_outcome((_replace(first, url=first.url + '#different'), second))
    assert (outcome.status, outcome.status_reason, outcome.rows) == (
        'error', 'attendance:pagination_identity_invalid', ())
    with pytest.raises(ValueError, match='non-conclusive'):
        core.plan_for_outcome(outcome, captured_at=PAGE_2_CAPTURED,
                              scope_values={'competition_id': 'GB1', 'edition_id': '2025'})


def test_finding_24b_unconvertible_agency_id_is_a_player_error():
    # Round 6 finding 24: the donor's integer conversion error stays inside extraction.
    payload = json.loads(_representation_capture().body)
    payload['data'][0]['attributes']['consultantAgencyId'] = '9' * 4301
    capture = _representation_capture(payload)
    failed, sibling = sections.extract_representation_observations(
        capture, _representation_scope(), expected_ids=('7', '9'),
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    assert (failed.status, failed.status_reason, failed.rows, failed.capture, failed.scope.player_id) == (
        'error', 'representation:invalid_packet', (), capture, '7')
    assert (sibling.status, sibling.scope.player_id) == ('unknown', '9')


def test_finding_25_donor_verdicts_are_rechecked_at_construction_dq_and_planning():
    # Round 6 finding 25: raw crowd cells, metric cells and representation evidence justify their verdicts.
    def refused(outcome, row, **changes):
        # Give each case a fresh row so it can change after LaneOutcome construction.
        rows = tuple(dict(original) for original in outcome.rows)
        index = outcome.rows.index(row)
        copied = _replace(outcome, rows=rows)
        rows[index].update(changes)
        with pytest.raises(CoreParseError, match='status_rows_mismatch'):
            _replace(copied)
        assert 'status_rows_mismatch' in core.build_core_dq([copied])['errors']
        values = {column: row[column] for column in core.CORE_SCOPE_COLUMNS[copied.entity]}
        with pytest.raises(CoreParseError, match='status_rows_mismatch'):
            core.plan_for_outcome(copied, captured_at=copied.capture.captured_at, scope_values=values)

    attendance = _attendance_outcome(_two_pages())
    for raw in ('?', ' UnKnOwN ', ''):
        refused(attendance, attendance.rows[0], spectators_total=None, spectators_average=None,
                spectators_total_raw='-', spectators_average_raw=raw, observation_status='explicit_none')
    refused(attendance, attendance.rows[0], spectators_total=None, spectators_average=None,
            spectators_total_raw='-', spectators_average_raw='-', observation_status='unknown')

    directory, = _referee_directory((_referee_directory_capture(),))
    metric = next(row for row in directory.rows if row['observation_status'] == 'observed_present')
    refused(directory, metric, value='14x')
    refused(directory, metric, observation_status='observed_empty', value=None, raw_value='-')
    refused(directory, metric, observation_status='explicit_none', value=None, raw_value='')

    assigned, _ = sections.extract_representation_observations(
        _representation_capture(), _representation_scope(), expected_ids=('7', '9'),
        extracted_at=EXTRACTED_AT, universe_version=UNIVERSE)
    refused(assigned, assigned.rows[0], representative_id='055', representative_key='agency:055')
    profile = 'https://www.transfermarkt.com/example/profil/spieler/7'
    absence = _replace(assigned, status='explicit_none', rows=({
        **assigned.rows[0], 'observation_status': 'explicit_none', 'representative_id': None,
        'representative_key': 'status:explicit_none', 'evidence_type': 'player_profile_explicit_none',
        'source_url': profile},))
    refused(absence, absence.rows[0], source_url=REPRESENTATION_URL)
    refused(absence, absence.rows[0], source_url=profile.replace('/7', '/9'))


def test_finding_26_representation_and_achievement_targets_use_canonical_ids():
    # Round 6 finding 26: the request, donor and write scope use the same positive decimal id.
    from scrapers.transfermarkt.orchestrator import AchievementHolderTarget

    plan = CoreScopePlan(league='GB1', season='2526', scope_id='s', cycle_id='c',
                         representation_player_ids=('7', '9'))
    replay = _JsonFetch(REPRESENTATION_URL, REPRESENTATION_PACKET)
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader, universe_version=UNIVERSE,
                  extracted_at=EXTRACTED_AT)
    expected = run.run(plan)
    db = CoreDB()
    report = run.run(_replace(plan, representation_player_ids=('007', '9')),
                     writer=CoreWriter(db), connection_factory=lambda: db)
    assert report.outcomes == expected.outcomes
    assert [item['status'] for item in report.outcomes] == ['observed_present', 'unknown']
    assert report.receipts[0]['verified'] is True

    replay = _JsonFetch(ACHIEVEMENTS_URL, ACHIEVEMENTS_PACKET)
    plan = _replace(plan, representation_player_ids=(),
                    achievement_holders=(AchievementHolderTarget('player', '007'),))
    run = CoreRun(replay.fetch, raw_loader=replay.raw_loader, universe_version=UNIVERSE,
                  extracted_at=EXTRACTED_AT)
    report = run.run(plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert report.outcomes[0]['status'] == 'observed_present'
    assert report.receipts[0]['verified'] is True
    assert db.sql.execute(
        'SELECT DISTINCT recipient_id FROM transfermarkt_achievement_observations').fetchall() == [('7',)]

    for value in ('', '000', '-1', '９'):
        with pytest.raises(CoreRunError, match='representation:player_id_invalid'):
            planned_urls(_replace(plan, achievement_holders=(), representation_player_ids=(value,)))
        with pytest.raises(CoreRunError, match='achievements:recipient_id_invalid'):
            planned_urls(_replace(plan, achievement_holders=(AchievementHolderTarget('player', value),)))


def test_finding_27_invalid_attendance_link_cites_its_own_page():
    # Round 6 finding 27: the source of a wrong-edition continuation backs the error.
    first, second = _two_pages()
    body = first.body.replace(b'saison_id/2025?page=2', b'saison_id/2023?page=2')
    assert body != first.body
    broken = _capture(first.url, body, first.captured_at)
    outcome = _attendance_outcome((broken, second))
    assert (outcome.status, outcome.status_reason, outcome.rows, outcome.capture) == (
        'error', 'attendance:pagination_identity_invalid', (), broken)


# ---------------------------------------------------------------- round 6: the acknowledged walk


def test_a_lost_manifest_is_restored_before_a_stale_walk_is_refused():
    # Round 7 finding 6: the journal restores the lost 12:30 evidence before
    # the cached 12:05 page is classified; repairing evidence never admits
    # the stale rows that the 12:30 walk had left out.
    page_1, cached_2 = _two_pages()
    empty_2 = _page(ATTENDANCE_PAGE_2, ROWLESS_TERMINAL_PAGE_2, NEWER_PAGE_CAPTURED, 'empty 12:30')
    plans = [_attendance_plan((page_1, empty_2))]
    db = CoreDB()
    first = _bundle(db, plans, EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11'}
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')

    stale = [_attendance_plan((page_1, cached_2), LATER_EXTRACTED_AT)]
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, stale, LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11'}
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone()[0] == 1
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_pages_v1').fetchone()[0] == 0
    # The rejected stale write changed nothing, including the ops tables.
    # The acknowledgement's own retry reads the same restored evidence and
    # neither logs another walk nor changes the business rows.
    writer = CoreWriter(db)
    repaired = native_writer.write_core_bundle(
        lambda: db, writer, plans, run_id=plans[0].rows[0]['run_id'], universe_version=UNIVERSE,
        committed_at=EXTRACTED_AT)
    assert repaired['verified'] is True and repaired['plan_sha256'] == first['plan_sha256']
    assert _manifest_evidence(db) == {
        ATTENDANCE_START: (page_1.capture_id, _moment(PAGE_1_CAPTURED), page_1.body_hash),
        ATTENDANCE_PAGE_2: (empty_2.capture_id, _moment(NEWER_PAGE_CAPTURED), empty_2.body_hash)}
    assert set(_stadiums(db)) == {'11'}
    # The lost clock is back: the cached page is stale again.
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, stale, LATER_EXTRACTED_AT)


def test_a_page_returns_with_the_rows_it_was_acknowledged_with():
    # Round 6 finding 5: a page that had left the walk was compared with an
    # empty row set when it returned, because the shorter walk that replaced
    # the scope no longer held its rows.
    page_1, page_2 = _two_pages()
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, page_2))], EXTRACTED_AT)
    single = _page(ATTENDANCE_START, (ATTENDANCE_FIXTURES / 'single_page.html').read_bytes(),
                   T1235, 'single 12:35')
    assert _bundle(db, [_attendance_plan((single,), LATER_EXTRACTED_AT)], LATER_EXTRACTED_AT)[
        'verified'] is True
    before = _stadiums(db)
    late_1 = _page(ATTENDANCE_START, page_1.body, T1245, 'late 12:45')
    returning = _attendance_plan((late_1, page_2), LATER_EXTRACTED_AT)
    # The capture that returns is the one the scope acknowledged: other rows
    # under its name are not that capture.
    changed = _replace(returning, rows=tuple(
        {**row, 'spectators_total': row['spectators_total'] + 1} if row['club_id'] == '631' else row
        for row in returning.rows))
    with pytest.raises(RuntimeError, match='replay differs'):
        _bundle(db, [changed], LATER_EXTRACTED_AT)
    assert _stadiums(db) == before

    assert _bundle(db, [returning], LATER_EXTRACTED_AT)['verified'] is True
    assert set(_stadiums(db)) == {'11', '631'}
    assert _manifest_evidence(db) == {
        ATTENDANCE_START: (late_1.capture_id, _moment(T1245), late_1.body_hash),
        ATTENDANCE_PAGE_2: (page_2.capture_id, _moment(PAGE_2_CAPTURED), page_2.body_hash)}


def test_each_acknowledgement_logs_its_walk_once():
    plans, page_1, empty_2 = _rowless_tail_snapshot()
    plan, = plans
    db = CoreDB()
    first = _write(db, plans, plan.rows[0]['run_id'], EXTRACTED_AT)
    # A row per page, stating the length of its walk and the rows the page
    # contributed: none for the page that left no row.
    assert db.sql.execute(
        'SELECT page_number, pages_count, source_capture_id, row_count '
        'FROM transfermarkt_core_pages_v1 ORDER BY page_number').fetchall() == [
        (1, 2, page_1.capture_id, 1), (2, 2, empty_2.capture_id, 0)]
    # The acknowledgement keeps its walk in the journal as well: each page
    # as planned, with the rows it contributed and their digest.
    walk, = first['walks']
    assert walk['table'] == plan.full_table and walk['scope_values_json'] == (
        '[["competition_id","GB1"],["edition_id","2025"]]')
    assert [[*page[:5], page[5], len(page[6])] for page in walk['pages']] == [
        [*plan.pages[0], 1, 64], [*plan.pages[1], 0, 64]]
    # An acknowledged replay neither writes nor logs anything again.
    assert _write(db, plans, plan.rows[0]['run_id'], EXTRACTED_AT) == first
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_pages_v1').fetchone() == (2,)


def test_a_walk_with_a_page_missing_is_restored_before_freshness_is_checked():
    # A partial walk is not evidence. The immutable acknowledgement restores
    # its missing page, and only the restored whole walk participates in the
    # freshness decision.
    page_1, cached_2 = _two_pages()
    empty_2 = _page(ATTENDANCE_PAGE_2, ROWLESS_TERMINAL_PAGE_2, NEWER_PAGE_CAPTURED, 'empty 12:30')
    db = CoreDB()
    _bundle(db, [_attendance_plan((page_1, empty_2))], EXTRACTED_AT)
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE page_number = 2')
    stale = [_attendance_plan((page_1, cached_2), LATER_EXTRACTED_AT)]
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, stale, LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11'}
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone() == (1,)
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_pages_v1').fetchone() == (1,)
    # The rejected write did not persist the repair. Its owner performs it
    # without another data replacement or a second receipt.
    assert _bundle(db, [_attendance_plan((page_1, empty_2))], EXTRACTED_AT)['verified'] is True
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_pages_v1').fetchone() == (2,)


def test_an_acknowledgement_cannot_repair_the_evidence_another_one_lost():
    # Only the acknowledgement that lost its pages may repair them on retry.
    # Another acknowledged unit still waits; a genuinely fresh write has a
    # separate restoration path and never repairs through this retry.
    page_1, page_2 = _two_pages()
    db = CoreDB()
    older = [_attendance_plan((page_1, page_2))]
    _bundle(db, older, EXTRACTED_AT)
    late_1 = _page(ATTENDANCE_START, page_1.body, T1215, 'late 12:15')
    newer = [_attendance_plan((late_1, page_2), LATER_EXTRACTED_AT)]
    _bundle(db, newer, LATER_EXTRACTED_AT)
    lost, = db.sql.execute(
        'SELECT receipt_id FROM transfermarkt_core_receipts_v1 ORDER BY committed_at').fetchall()[1:]
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE receipt_id = ?', lost)
    with pytest.raises(RuntimeError, match='manifest lost'):
        _bundle(db, older, EXTRACTED_AT)
    # The newer acknowledgement's retry restores its own walk.
    assert _bundle(db, newer, LATER_EXTRACTED_AT)['verified'] is True
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_pages_v1').fetchone() == (4,)
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, older, EXTRACTED_AT)


# --------------------------------- review round-7 pins


def test_finding_12_capture_conflict_with_an_error_profile_prevents_all_writes():
    # Round 7 finding 12: a failed profile still binds its capture ID before any bundle writes.
    profile = _referee_profile_capture()
    other_url = REFEREE_PROFILE_URL.replace('/101', '/102')
    replay = _SectionFetch({profile.url: profile.body, other_url: b'<html></html>'})

    def fetch(url, **kwargs):
        return _replace(replay.fetch(url, **kwargs), raw_capture_id=profile.capture_id)

    plan = CoreScopePlan(league='GB1', season='2627', scope_id='s', cycle_id='c',
                         referee_profile_ids=('101', '102'))
    db = CoreDB()
    writer = CoreWriter(db)
    with pytest.raises(CoreRunError, match='^capture_evidence_conflict$'):
        CoreRun(fetch, extracted_at=EXTRACTED_AT).run(plan, writer=writer, connection_factory=lambda: db)
    assert replay.calls == [profile.url, other_url]
    assert writer.writes == []
    assert db.sql.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'transfermarkt_%'"
    ).fetchall() == []


def test_finding_14_invalid_agency_id_only_withholds_its_players_receipt():
    # Round 7 finding 14: a bad agency value preserves its capture and a valid sibling's receipt.
    payload = json.loads(_representation_capture().body)
    payload['data'][0]['attributes']['consultantAgencyId'] = '9' * 4301
    payload['data'][1]['attributes']['consultantAgencyId'] = '56'
    replay = _JsonFetch(REPRESENTATION_URL, payload)
    plan = CoreScopePlan(league='GB1', season='2627', scope_id='s', cycle_id='c',
                         representation_player_ids=('7', '9'))
    db = CoreDB()
    report = CoreRun(replay.fetch, raw_loader=replay.raw_loader, extracted_at=EXTRACTED_AT).run(
        plan, writer=CoreWriter(db), connection_factory=lambda: db)
    failed, assigned = report.outcomes
    assert (failed['status'], failed['status_reason'], failed['rows'], failed['source_capture_id']) == (
        'error', 'representation:invalid_packet', 0, _representation_capture(payload).capture_id)
    assert (assigned['status'], assigned['rows']) == ('observed_present', 1)
    assert db.sql.execute(
        'SELECT player_id, representative_id FROM transfermarkt_player_representation_observations'
    ).fetchall() == [('9', '56')]
    receipt, = report.receipts
    assert receipt['verified'] is True
    (scope_json,), = db.sql.execute('SELECT scope_values_json FROM transfermarkt_core_receipts_v1')
    assert json.loads(scope_json) == [['player_id', '9']]


def test_finding_15_skipped_attendance_page_is_contained_and_referee_verifies():
    # Round 7 finding 15: navigation cites the bad link, and replay rejects the out-of-order page.
    pages = _attendance_pages()
    first = _capture(ATTENDANCE_START, pages[ATTENDANCE_START].replace(b'?page=2', b'?page=3'), PAGE_1_CAPTURED)
    third = _capture(ATTENDANCE_START + '?page=3', pages[ATTENDANCE_PAGE_2], PAGE_2_CAPTURED)
    outcome = _attendance_outcome((first, third))
    assert (outcome.status, outcome.status_reason, outcome.rows, outcome.capture) == (
        'error', 'attendance:pagination_not_sequential', (), third)

    profile = _referee_profile_capture()
    replay = _SectionFetch({page.url: page.body for page in (first, third, profile)})
    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),),
        referee_profile_ids=('101',))
    db = CoreDB()
    report = CoreRun(replay.fetch, extracted_at=EXTRACTED_AT).run(
        plan, writer=CoreWriter(db), connection_factory=lambda: db)
    assert replay.calls == [first.url, profile.url]
    failed, written = report.outcomes
    assert (failed['entity'], failed['status'], failed['status_reason'], failed['rows'],
            failed['source_capture_id']) == (
        'attendance_observations', 'error', 'attendance:pagination_not_sequential', 0, first.capture_id)
    assert (written['entity'], written['status']) == ('referee_profiles', 'observed_present')
    receipt, = report.receipts
    assert receipt['verified'] is True
    assert db.sql.execute('SELECT referee_id FROM transfermarkt_referee_profiles').fetchall() == [('101',)]


@pytest.mark.parametrize('conflict', [True, False])
def test_r8_discarded_attendance_page_binds_its_capture_id(conflict):
    # Round 8 finding 16
    first, second = _two_pages()
    broken = _capture(second.url, second.body.replace(b'40,173', b'not-a-number'), second.captured_at)
    profile = _referee_profile_capture()
    replay = _SectionFetch({page.url: page.body for page in (first, broken, profile)})

    def fetch(url, **kwargs):
        result = replay.fetch(url, **kwargs)
        if conflict and url == profile.url:
            return _replace(result, raw_capture_id=first.capture_id)
        return result

    plan = CoreScopePlan(
        league='GB1', season='2526', scope_id='s', cycle_id='c',
        attendance_editions=(AttendanceEditionTarget('GB1', 2025, 'premier-league'),),
        referee_profile_ids=('101',))
    db = CoreDB()
    writer = CoreWriter(db)
    run = CoreRun(fetch, extracted_at=EXTRACTED_AT)
    if conflict:
        with pytest.raises(CoreRunError, match='^capture_evidence_conflict$'):
            run.run(plan, writer=writer, connection_factory=lambda: pytest.fail('unexpected connection'))
        assert writer.writes == []
    else:
        report = run.run(plan, writer=writer, connection_factory=lambda: db)
        failed, valid = report.outcomes
        assert (failed['status'], failed['source_capture_id']) == ('error', broken.capture_id)
        assert (valid['entity'], valid['status']) == ('referee_profiles', 'observed_present')
        receipt, = report.receipts
        assert receipt['verified'] is True
    assert replay.calls == [first.url, broken.url, profile.url]


def test_r8_unquoted_oversized_agency_preserves_the_sibling_receipt(tmp_path):
    # Round 8 finding 19
    from dags.scripts import run_transfermarkt_core as cli
    from tests.unit.scrapers.test_transfermarkt_orchestrator import _write_replay_manifest

    payload = json.loads(_representation_capture().body)
    payload['data'][0]['attributes']['consultantAgencyId'] = 'oversized'
    payload['data'][1]['attributes']['consultantAgencyId'] = 56
    digits = '9' * 4301
    body = json.dumps(payload).replace('"oversized"', digits).encode()
    capture = _capture(REPRESENTATION_URL, body, CLUB_CAPTURED, endpoint='representation')
    _write_replay_manifest(tmp_path, [capture])
    records = cli._load_records(tmp_path)
    fixture_fetch = cli._fixture_fetch(records)

    def fetch(url, **kwargs):
        result = fixture_fetch(url, **kwargs)
        assert result.status is FetchStatus.OK
        first, second = result.value['data']
        assert first['attributes']['consultantAgencyId'] == digits
        assert second['attributes']['consultantAgencyId'] == 56
        return result

    plan = CoreScopePlan(league='GB1', season='2627', scope_id='s', cycle_id='c',
                         representation_player_ids=('7', '9'))
    db = CoreDB()
    report = CoreRun(fetch, request_raw_loader=cli._fixture_raw_loader(records), extracted_at=EXTRACTED_AT).run(
        plan, writer=CoreWriter(db), connection_factory=lambda: db)
    failed, assigned = report.outcomes
    assert (failed['status'], failed['status_reason'], failed['rows'], failed['source_capture_id']) == (
        'error', 'representation:invalid_packet', 0, capture.capture_id)
    assert (assigned['status'], assigned['rows']) == ('observed_present', 1)
    receipt, = report.receipts
    assert receipt['verified'] is True
    assert db.sql.execute(
        'SELECT player_id, representative_id FROM transfermarkt_player_representation_observations'
    ).fetchall() == [('9', '56')]


def test_walk_order_refuses_different_evidence_at_an_identical_pair_of_clocks():
    # Round 8 finding 11
    db = CoreDB()
    first, second = _two_pages()
    original = _attendance_plan((first, second))
    changed = _page(first.url, first.body, first.captured_at, 'same clock, other capture')
    collision = _attendance_plan((changed, second), LATER_EXTRACTED_AT)
    receipt = _bundle(db, [original], EXTRACTED_AT)
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, [collision], EXTRACTED_AT)
    # Retrying the conflicting pending unit keeps its first acknowledgement clock.
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, [collision], LATER_EXTRACTED_AT)
    assert _bundle(db, [original], LATER_EXTRACTED_AT) == receipt
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone() == (1,)


def test_journal_only_walk_still_guards_a_rowless_page_and_is_restored_on_commit():
    # Round 8 finding 12
    db = CoreDB()
    first, cached = _two_pages()
    empty = _page(cached.url, ROWLESS_TERMINAL_PAGE_2, NEWER_PAGE_CAPTURED, 'empty tail')
    receipt = _bundle(db, [_attendance_plan((first, empty))], EXTRACTED_AT)
    receipt_id = receipt['walks'][0]['receipt_id']
    original = native_writer._stored_walk(db, receipt_id)
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')
    db.sql.execute('DELETE FROM transfermarkt_core_receipts_v1')
    fresh_first = _page(first.url, first.body, T1245, 'fresh head')
    stale = _attendance_plan((fresh_first, cached), LATER_EXTRACTED_AT)
    with pytest.raises(RuntimeError, match='manifest lost'):
        native_writer._replay_state(db, stale)
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, [stale], LATER_EXTRACTED_AT)
    assert native_writer._stored_walk(db, receipt_id) is None
    assert set(_stadiums(db)) == {'11'}
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_receipts_v1').fetchone() == (0,)
    fresh = _attendance_plan((fresh_first, empty), LATER_EXTRACTED_AT)
    assert _bundle(db, [fresh], LATER_EXTRACTED_AT)['verified']
    assert native_writer._stored_walk(db, receipt_id) == original
    assert set(_stadiums(db)) == {'11'}


@pytest.mark.parametrize('rowless', [False, True])
def test_numeric_page_identity_guards_cached_padded_page_two(rowless):
    # Round 8 finding 13
    db = CoreDB()
    first, cached = _two_pages()
    tail_body = ROWLESS_TERMINAL_PAGE_2 if rowless else cached.body
    tail = _page(cached.url, tail_body, NEWER_PAGE_CAPTURED, 'fresh tail')
    original = _attendance_plan((first, tail))
    _bundle(db, [original], EXTRACTED_AT)
    padded_url = ATTENDANCE_START + '?page=02'
    padded_head = _page(first.url, first.body.replace(b'page=2', b'page=02'), T1245, 'padded head')
    stale = _attendance_plan((padded_head, _page(padded_url, cached.body, PAGE_2_CAPTURED, 'padded tail')),
                             LATER_EXTRACTED_AT)
    assert stale.pages[1][0] == padded_url
    if not rowless:
        with pytest.raises(StaleTransfermarktWrite):
            native_writer._guard_source_pages(db, stale, {})
    with pytest.raises(StaleTransfermarktWrite):
        _bundle(db, [stale], LATER_EXTRACTED_AT)
    fresh = _attendance_plan((padded_head, _page(padded_url, cached.body, T1245, 'refreshed padded tail')),
                             LATER_EXTRACTED_AT)
    accepted = _bundle(db, [fresh], LATER_EXTRACTED_AT)
    assert accepted['walks'][0]['pages'][1][0] == padded_url
    pages = native_writer._accepted_pages(db, fresh).pages
    assert set(pages) == {('GB1', '2025', 1), ('GB1', '2025', 2)}
    assert set(_stadiums(db)) == {'11', '631'}


@pytest.mark.parametrize('version', [6, 7, 8])
def test_walk_digest_keeps_original_cells_and_survives_a_new_scope_and_universe(monkeypatch, version):
    # Round 8 finding 14
    db = CoreDB()
    pages = _two_pages()
    first = _attendance_plan(pages)
    with monkeypatch.context() as old:
        if version < 8:
            ignored = {'run_id', '_batch_id', '_ingested_at', 'extracted_at'}
            if version == 7:
                ignored.add('cycle_id')

            def old_digest(plan, page, rows, *_):
                return native_writer._digest([
                    {k: v for k, v in row.items() if k not in ignored}
                    for row in native_writer._sorted_rows(rows, plan.natural_keys)])

            record_walks = native_writer._journal_walks

            def old_walks(plans, ids):
                walks = record_walks(plans, ids)
                for walk in walks:
                    del walk['digest_version']
                return walks

            old.setattr(native_writer, '_page_digest', old_digest)
            old.setattr(native_writer, '_journal_walks', old_walks)
        receipt = _bundle(db, [first], EXTRACTED_AT)

    key = receipt['walks'][0]['receipt_id']
    original = native_writer._stored_walk(db, key)
    assert _bundle(db, [first], EXTRACTED_AT) == receipt
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1 WHERE page_number = 2')
    assert _bundle(db, [first], EXTRACTED_AT) == receipt
    assert native_writer._stored_walk(db, key) == original

    scope = _replace(_attendance_scope(), scope_id='another-scope', cycle_id='another-cycle')
    outcome, = sections.extract_attendance_observations(
        pages, scope, start_url=ATTENDANCE_START, competition_id='GB1', edition_id='2025',
        extracted_at=LATER_EXTRACTED_AT, universe_version='another-universe')
    cached = core.plan_for_outcome(outcome, captured_at=first.captured_at,
                                   scope_values=dict(zip(first.scope_columns, first.scope_values)))

    def commit(plan):
        return native_writer.write_core_bundle(lambda: db, CoreWriter(db), [plan], run_id=outcome.run_id,
                                              universe_version=outcome.universe_version, committed_at=LATER_EXTRACTED_AT)

    changed = _replace(cached, rows=tuple({**row, 'capacity': row['capacity'] + 1} for row in cached.rows))
    with pytest.raises(RuntimeError, match='replay differs'):
        commit(changed)
    fresh = commit(cached)
    assert fresh['walks'][0]['digest_version'] == 8
    assert fresh['universe_version'] == 'another-universe' and fresh['run_id'] == outcome.run_id
    assert native_writer._stored_walk(db, key) == original
    old_hashes = [page[6] for page in receipt['walks'][0]['pages']]
    new_hashes = [page[6] for page in fresh['walks'][0]['pages']]
    assert (old_hashes == new_hashes) == (version == 8)


def test_a_fence_on_the_second_lost_walk_prevents_the_first_repair():
    # Round 8 finding 15
    journal = native_writer.write_journal
    db = CoreDB()
    body = (ATTENDANCE_FIXTURES / 'single_page.html').read_bytes()

    def single(clock):
        return _attendance_plan((_page(ATTENDANCE_START, body, clock, clock),))

    keys = [_write(db, [single(clock)], clock, clock)['walks'][0]['receipt_id']
            for clock in (PAGE_1_CAPTURED, NEWER_PAGE_CAPTURED)]
    before = {key: native_writer._stored_walk(db, key) for key in keys}
    receipt_rows = db.sql.execute('SELECT * FROM transfermarkt_core_receipts_v1 ORDER BY receipt_id').fetchall()
    later_key = max(keys)
    fence = journal.begin_statement((native_writer.CORE_PAGES_TABLE, later_key),
                                    {'single_submit': native_writer._digest(before[later_key])})
    journal.remember_query(fence, 'queued-walk')
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')
    business = db.sql.execute('SELECT * FROM transfermarkt_attendance_observations').fetchall()
    cursor = len(db.events)
    with pytest.raises(RuntimeError, match='still pending'):
        _write(db, [single(T1245)], T1245, T1245)
    assert not any(sql.startswith('INSERT') for sql in db.events[cursor:])
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_pages_v1').fetchone() == (0,)
    assert db.sql.execute('SELECT * FROM transfermarkt_attendance_observations').fetchall() == business
    assert db.sql.execute('SELECT * FROM transfermarkt_core_receipts_v1 ORDER BY receipt_id').fetchall() == receipt_rows
    db.sql.execute("INSERT INTO test_query_history VALUES ('queued-walk', 'FAILED', 'QUERY_QUEUE_FULL')")
    assert _write(db, [single(T1245)], T1245, T1245)['verified']
    assert all(native_writer._stored_walk(db, key) == rows for key, rows in before.items())


def test_missing_unscoped_legacy_walk_cannot_be_skipped(monkeypatch):
    # Round 8 finding 12
    db = CoreDB()
    original = _attendance_plan(_two_pages())
    journal = native_writer.write_journal
    record = journal.record_receipt

    def old_ack(key, receipt):
        proofs = [{k: v for k, v in proof.items() if k in (
            'entity', 'table', 'rows', 'physical_hash', 'expected_rows', 'status_counts')}
                  for proof in receipt['plans']]
        record(key, {**receipt, 'plans': proofs, 'walks': []})

    with monkeypatch.context() as patch:
        patch.setattr(journal, 'record_receipt', old_ack)
        _bundle(db, [original], EXTRACTED_AT)
    db.sql.execute('DELETE FROM transfermarkt_core_pages_v1')
    db.sql.execute('DELETE FROM transfermarkt_core_receipts_v1')
    fresh = _attendance_plan((_attendance_capture('single_page.html', ATTENDANCE_START, T1245),))
    with pytest.raises(RuntimeError, match='manifest lost'):
        _bundle(db, [fresh], LATER_EXTRACTED_AT)
    assert set(_stadiums(db)) == {'11', '631'}
    assert db.sql.execute('SELECT COUNT(*) FROM transfermarkt_core_pages_v1').fetchone() == (0,)
