"""Stage-C section lanes: attendance (T10) and club values (T11) observations (#2405)."""
import hashlib
from pathlib import Path

import pytest

from scrapers.transfermarkt import core, sections
from scrapers.transfermarkt.attendance import AttendancePage
from scrapers.transfermarkt.core import CoreParseError, LaneOutcome, ScopeContext, SourceCapture
from scrapers.transfermarkt.models import FetchOutcome, FetchStatus
from scrapers.transfermarkt.orchestrator import (
    AttendanceEditionTarget,
    ClubValueTarget,
    CoreRun,
    CoreScopePlan,
    attendance_url,
    club_values_url,
    planned_urls,
)
from scrapers.transfermarkt.native_writer import NativeWritePlan
from tests.unit.scrapers.test_transfermarkt_native_writer import CoreDB, CoreWriter

pytestmark = pytest.mark.unit

ATTENDANCE_FIXTURES = Path('tests/fixtures/transfermarkt/attendance')
CLUB_VALUE_FIXTURES = Path('tests/fixtures/transfermarkt/club_values')
ATTENDANCE_START = 'https://www.transfermarkt.com/premier-league/besucherzahlen/wettbewerb/GB1/saison_id/2025'
ATTENDANCE_PAGE_2 = ATTENDANCE_START + '?page=2'
CLUB_VALUE_URL = 'https://www.transfermarkt.com/v-league-1/marktwerteverein/wettbewerb/VIE1'
PAGE_1_CAPTURED = '2026-11-20T12:00:00+00:00'
PAGE_2_CAPTURED = '2026-11-20T12:05:00+00:00'
CLUB_CAPTURED = '2026-11-20T10:00:00+00:00'
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
                           ({'observation_status': 'unknown'}), **context)
    assert accepted.status == 'unknown'
    # mixed per-row statuses stay valid under an observed_present outcome
    mixed = LaneOutcome('attendance_observations', 'observed_present', 'attendance:ok',
                        ({'observation_status': 'observed_present'},
                         {'observation_status': 'explicit_none'}), **context)
    assert len(mixed.rows) == 2
    # a row status outside the section policy is still rejected
    with pytest.raises(CoreParseError):
        LaneOutcome('attendance_observations', 'observed_present', 'attendance:ok',
                    ({'observation_status': 'observed_empty'}), **context)
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
        return FetchOutcome(
            status=FetchStatus.OK, value=body.decode('utf-8'), raw_capture_id=digest,
            raw_body_hash=digest, raw_fetched_at=PAGE_1_CAPTURED, cache_hit=False,
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
