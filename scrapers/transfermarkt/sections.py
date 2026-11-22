"""Map the frozen stage-C section parsers into the core observation contract.

The donors interpret source pages. These adapters preserve their row verdicts
and bind each observation to the capture and run that produced it.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
import json

from bs4 import ParserRejectedMarkup

from . import achievements, attendance, club_values, core


def extract_attendance_observations(
    captures: Sequence[core.SourceCapture], scope: core.ScopeContext, *, start_url: str,
    competition_id: str, edition_id: str, extracted_at: str, universe_version: str,
    final_season: bool = False, final_season_proof: str | None = None,
) -> core.LaneOutcome:
    entity = 'attendance_observations'
    if not captures or any(capture.endpoint != 'attendance' for capture in captures):
        raise core.CoreParseError('attendance:invalid_captures')
    if not isinstance(final_season, bool):
        raise core.CoreParseError('attendance:final_season_invalid')
    if final_season and not core._nonempty(final_season_proof):
        raise core.CoreParseError('attendance:final_season_proof_missing')

    context = core._context(captures[-1], scope, extracted_at, universe_version)
    try:
        pages = [
            attendance.AttendancePage(
                url=capture.url, body=capture.body, capture_id=capture.capture_id,
                body_hash=capture.body_hash, status_code=capture.status_code,
                # The donor's ISO format omits zero microseconds. Row lineage uses the original.
                captured_at=datetime.fromisoformat(capture.captured_at).isoformat(),
            )
            for capture in captures
        ]
        result = attendance.parse_attendance(
            pages, start_url=start_url, competition_id=competition_id, edition_id=edition_id,
        )
    except attendance.AttendanceParseError as exc:
        return core.LaneOutcome(entity, 'error', 'attendance:' + exc.reason, **context)
    except (ValueError, OverflowError):
        return core.LaneOutcome(entity, 'error', 'attendance:invalid_source_data', **context)

    newest = max(captures, key=lambda capture: (capture.captured_at, capture.capture_id))
    context['capture'] = newest
    by_id = {capture.capture_id: capture for capture in captures}
    reason = 'attendance:' + result.status_reason
    rows = []
    for row in result.rows:
        try:
            row_capture = by_id[row.source_capture_id]
        except KeyError as exc:
            raise core.CoreParseError('attendance:row_capture_missing') from exc
        values = row.semantic_dict()
        values.update(final_season=final_season,
                      final_season_proof=final_season_proof if final_season else '')
        rows.append(core._render(
            entity, values, scope, capture=row_capture, run_id=context['run_id'],
            extracted_at=extracted_at, universe_version=universe_version,
            observation_status=row.observation_status, status_reason=reason,
            parser_revision=attendance.ATTENDANCE_PARSER_REVISION,
            schema_revision=attendance.ATTENDANCE_SCHEMA_REVISION,
        ))
    return core.LaneOutcome(entity, result.status, reason, tuple(rows), **context)


def extract_club_value_observations(
    capture: core.SourceCapture, scope: core.ScopeContext, *, extracted_at: str, universe_version: str,
) -> tuple[core.LaneOutcome, ...]:
    entity = 'club_value_observations'
    if capture.endpoint != 'club_values':
        raise core.CoreParseError('club_values:invalid_endpoint')
    context = core._context(capture, scope, extracted_at, universe_version)
    try:
        club_capture = club_values.ClubValueCapture(
            competition_id=scope.league, source_url=capture.url, raw_capture_id=capture.capture_id,
            source_body_hash=capture.body_hash, captured_at=capture.captured_at,
            extracted_at=extracted_at, scope_id=scope.scope_id, run_id=context['run_id'],
            cycle_id=scope.cycle_id, universe_version=universe_version,
        )
        extraction = club_values.parse_club_values(capture.text, club_capture)
    except club_values.ClubValueParseError as exc:
        return (core.LaneOutcome(entity, 'error', 'club_values:' + exc.reason, **context),)
    except UnicodeDecodeError:
        return (core.LaneOutcome(entity, 'error', 'club_values:invalid_utf8', **context),)
    except (ValueError, TypeError, ArithmeticError, RecursionError):
        return (core.LaneOutcome(entity, 'error', 'club_values:invalid_source_data', **context),)

    by_club = {}
    for point in extraction.rows:
        row = core._render(
            entity, point, observation_status=point['observation_status'], status_reason='club_values:point',
            parser_revision=club_values.CLUB_VALUE_PARSER_REVISION,
            schema_revision=club_values.CLUB_VALUE_SCHEMA_REVISION, **context,
        )
        by_club.setdefault(point['club_id'], []).append(row)

    outcomes = []
    for rows in by_club.values():
        statuses = {row['observation_status'] for row in rows}
        if 'observed_present' in statuses:
            status, reason = 'observed_present', 'club_rows_present'
        elif statuses == {'explicit_none'}:
            status, reason = 'explicit_none', 'club_rows_explicit_none'
        else:
            status, reason = 'unknown', 'club_rows_unresolved'
        outcomes.append(core.LaneOutcome(entity, status, 'club_values:' + reason, tuple(rows), **context))
    return tuple(outcomes)


def extract_achievement_observations(
    capture: core.SourceCapture, scope: core.ScopeContext, *, recipient_type: str, recipient_id: str,
    extracted_at: str, universe_version: str,
) -> core.LaneOutcome:
    entity = 'achievement_observations'
    if capture.endpoint != f'{recipient_type}_achievement':
        raise core.CoreParseError('achievements:invalid_endpoint')
    core._check_scope_against_capture(
        entity, {'recipient_type': recipient_type, 'recipient_id': recipient_id}, capture,
    )
    context = core._context(capture, scope, extracted_at, universe_version)
    try:
        source = capture.text if recipient_type == 'coach' else json.loads(capture.text)
    except (ValueError, RecursionError):
        reason = 'achievements:invalid_utf8' if recipient_type == 'coach' else 'achievements:invalid_json'
        return core.LaneOutcome(entity, 'error', reason, **context)
    try:
        result = achievements.parse_achievements(source, recipient_type=recipient_type, recipient_id=recipient_id)
    except achievements.AchievementParseError as exc:
        return core.LaneOutcome(entity, 'error', 'achievements:' + exc.reason, **context)
    except (ParserRejectedMarkup, ValueError, RecursionError):
        return core.LaneOutcome(entity, 'error', 'achievements:invalid_source_data', **context)

    if result.status == 'observed_empty':
        return core.LaneOutcome(entity, result.status, 'achievements:empty_collection',
                                proof=f'achievements:empty_collection:{recipient_type}', **context)

    reason = 'achievements:row'
    rows = []
    for achievement in result.rows:
        values = achievement.as_dict()
        if values['recipient_id'] != recipient_id or values['recipient_type'] != recipient_type:
            raise core.CoreParseError('row_scope_mismatch')
        rows.append(core._render(
            entity, values, observation_status='observed_present', status_reason=reason,
            parser_revision=achievements.ACHIEVEMENT_PARSER_REVISION,
            schema_revision=str(achievements.ACHIEVEMENT_SCHEMA_REVISION), **context,
        ))
    return core.LaneOutcome(entity, result.status, reason, tuple(rows), **context)
