"""Stage-C section lanes: attendance (T10), club values (T11), achievements (T8) (#2405)."""
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
    return core.plan_for_outcome(
        outcome, captured_at=max(capture.captured_at for capture in captures),
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
                    ({'observation_status': 'observed_present', 'parser_revision': 'other-v9'}),
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
