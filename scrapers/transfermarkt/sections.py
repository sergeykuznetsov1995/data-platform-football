"""Map the frozen stage-C section parsers into the core observation contract.

The donors interpret source pages. These adapters preserve their row verdicts
and bind each observation to the capture and run that produced it.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
import json
from urllib.parse import urlsplit

from bs4 import ParserRejectedMarkup

from . import (
    achievements, attendance, club_values, core, referees, representation, rumour_observations, rumours,
)


def _unavailable_capture(section: str, entity: str, capture: core.SourceCapture,
                         scope: core.ScopeContext, extracted_at: str, universe_version: str):
    """A non-200 body is transport evidence, never section content."""
    context = core._context(capture, scope, extracted_at, universe_version)
    return core.LaneOutcome(entity, 'unavailable', f'{section}:http_{capture.status_code}', **context)


def extract_attendance_observations(
    captures: Sequence[core.SourceCapture], scope: core.ScopeContext, *, start_url: str,
    competition_id: str, edition_id: str, extracted_at: str, universe_version: str,
    final_season: bool = False, final_season_proof: str | None = None,
) -> core.LaneOutcome:
    entity = 'attendance_observations'
    if not captures or any(capture.endpoint != 'attendance' for capture in captures):
        raise core.CoreParseError('attendance:invalid_captures')
    failed = next((c for c in captures if c.status_code != 200), None)
    if failed is not None:
        return _unavailable_capture('attendance', 'attendance_observations', failed, scope,
                                    extracted_at, universe_version)
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
            observation_status=row.observation_status,
            status_reason='attendance:' + row.status_reason,
            parser_revision=attendance.ATTENDANCE_PARSER_REVISION,
            schema_revision=attendance.ATTENDANCE_SCHEMA_REVISION,
        ))
    if rows:
        # The outcome's capture is the newest page that CONTRIBUTED a row: a
        # newer but rowless trailing page must not shift the plan's clock.
        contributing = {row['source_capture_id'] for row in rows}
        context['capture'] = max(
            (capture for capture in captures if capture.capture_id in contributing),
            key=lambda capture: (capture.captured_at, capture.capture_id))
    return core.LaneOutcome(entity, result.status, reason, tuple(rows), **context)


def extract_representation_observations(
    capture: core.SourceCapture, scope: core.ScopeContext, *, expected_ids: Sequence[str],
    extracted_at: str, universe_version: str,
) -> tuple[core.LaneOutcome, ...]:
    entity = 'player_representation_observations'
    if capture.endpoint != 'representation':
        raise core.CoreParseError('representation:invalid_endpoint')
    if capture.status_code != 200:
        return _unavailable_capture('representation', 'player_representation_observations', capture, scope,
                                    extracted_at, universe_version)
    context = core._context(capture, scope, extracted_at, universe_version)
    try:
        try:
            payload = json.loads(capture.text)
        except (ValueError, RecursionError) as exc:
            raise representation.RepresentationParseError('invalid_json') from exc
        assertions = representation.parse_representation_packet(payload, expected_ids=expected_ids)
    except representation.RepresentationParseError as exc:
        return (core.captureless_outcome(
            entity, scope, status='error', reason='representation:' + exc.reason,
            extracted_at=extracted_at, run_id=context['run_id'], universe_version=universe_version,
        ),)

    outcomes = []
    for player_id in sorted(assertions, key=int):
        assertion = assertions[player_id]
        core._check_scope_against_capture(entity, {'player_id': player_id}, capture)
        player_context = context | {'scope': replace(scope, player_id=player_id)}
        status = assertion.observation_status
        row_reason = 'representation:' + assertion.status_reason
        if status == 'error':
            outcomes.append(core.LaneOutcome(entity, status, row_reason, **player_context))
            continue
        if status == 'assigned':
            status = 'observed_present'
            reason = 'representation:assigned'
            representative_key = f'agency:{assertion.representative_id}'
        else:
            reason = row_reason
            representative_key = f'status:{status}'
        values = {
            'player_id': player_id, 'relation_type': representation.RELATION_TYPE,
            'representative_key': representative_key, 'representative_id': assertion.representative_id,
            'observation_status': status, 'status_reason': row_reason,
            'evidence_type': assertion.evidence_type, 'evidence_locator': assertion.evidence_locator,
            'observed_at': capture.captured_at,
        }
        row = core._render(
            entity, values, observation_status=status, status_reason=row_reason,
            parser_revision=representation.REPRESENTATION_PARSER_REVISION,
            schema_revision=representation.REPRESENTATION_SCHEMA_REVISION, **player_context,
        )
        outcomes.append(core.LaneOutcome(entity, status, reason, (row,), **player_context))
    return tuple(outcomes)


def extract_referee_directory_observations(
    captures: Sequence[core.SourceCapture], scope: core.ScopeContext, *,
    extracted_at: str, universe_version: str,
) -> tuple[core.LaneOutcome, ...]:
    entity = 'referee_aggregate_observations'
    if not captures or any(capture.endpoint != 'referee_directory' for capture in captures):
        raise core.CoreParseError('referees:invalid_captures')
    failed = next((c for c in captures if c.status_code != 200), None)
    if failed is not None:
        return (_unavailable_capture('referees', 'referee_aggregate_observations', failed, scope,
                                     extracted_at, universe_version),)
    competition_id, edition_id = core._referee_directory_identity(urlsplit(captures[0].url).path)
    directory = {'competition_id': competition_id, 'edition_id': edition_id}
    for capture in captures[1:]:
        core._check_scope_against_capture(entity, directory, capture)

    newest = max(captures, key=lambda capture: (capture.captured_at, capture.capture_id))
    context = core._context(newest, scope, extracted_at, universe_version)
    first_page = {}
    try:
        pages = [capture.text for capture in captures]
        for index, html in enumerate(pages):
            page = referees.parse_referee_directory_page(html, **directory)
            for aggregate in page.aggregates:
                first_page.setdefault(aggregate.natural_key, index)
        result = referees.reconcile_referee_directory_pages(
            pages, expected_page_count=len(pages), **directory,
        )
    except referees.RefereeParseError as exc:
        return (core.LaneOutcome(entity, 'error', 'referees:' + exc.reason, **context),)
    except ValueError as exc:
        return (core.LaneOutcome(entity, 'error', 'referees:' + (str(exc) or 'invalid_source_data'), **context),)

    if not result.aggregates:
        if result.completeness_proof:
            proof = 'referees:' + result.completeness_proof
            return (core.LaneOutcome(entity, 'observed_empty', proof, proof=proof, **context),)
        return (core.LaneOutcome(entity, 'error', 'referees:completeness_proof_missing', **context),)

    rows_by_page = [[] for _ in captures]
    for aggregate in result.aggregates:
        index = first_page[aggregate.natural_key]
        capture = captures[index]
        values = aggregate.as_dict()
        values['observed_at'] = capture.captured_at
        rows_by_page[index].append(core._render(
            entity, values, scope, capture, extracted_at, context['run_id'], universe_version,
            observation_status=aggregate.observation_status, status_reason='referees:' + aggregate.status_reason,
            parser_revision=referees.REFEREE_PARSER_REVISION, schema_revision=referees.REFEREE_SCHEMA_REVISION,
        ))
    return tuple(
        core.LaneOutcome(entity, 'observed_present', 'referees:page', tuple(rows),
                         **core._context(capture, scope, extracted_at, universe_version))
        for capture, rows in zip(captures, rows_by_page) if rows
    )


def extract_referee_profile_observations(
    capture: core.SourceCapture, scope: core.ScopeContext, *, referee_id: str,
    extracted_at: str, universe_version: str,
) -> core.LaneOutcome:
    entity = 'referee_profiles'
    if capture.endpoint != 'referee_profile':
        raise core.CoreParseError('referees:invalid_endpoint')
    if capture.status_code != 200:
        return _unavailable_capture('referees', 'referee_profiles', capture, scope,
                                    extracted_at, universe_version)
    core._check_scope_against_capture(entity, {'referee_id': referee_id}, capture)
    context = core._context(capture, scope, extracted_at, universe_version)
    try:
        profile = referees.parse_referee_profile(capture.text, referee_id=referee_id)
    except referees.RefereeParseError as exc:
        return core.LaneOutcome(entity, 'error', 'referees:' + exc.reason, **context)
    except UnicodeDecodeError:
        return core.LaneOutcome(entity, 'error', 'referees:invalid_utf8', **context)

    reason = 'referees:profile'
    values = profile.as_dict()
    values['observed_at'] = capture.captured_at
    row = core._render(
        entity, values, observation_status='observed_present', status_reason=reason,
        parser_revision=referees.REFEREE_PARSER_REVISION, schema_revision=referees.REFEREE_SCHEMA_REVISION,
        **context,
    )
    return core.LaneOutcome(entity, 'observed_present', reason, (row,), **context)


def extract_rumour_observations(
    captures: Sequence[core.SourceCapture], scope: core.ScopeContext, *,
    competition: rumours.CompetitionContext, extracted_at: str, universe_version: str,
) -> tuple[core.LaneOutcome, ...]:
    """Observe one COMPLETE rumour feed, one write-plan outcome per page.

    A merge plan owns exactly one capture, so each page's rows become their own
    outcome inside the one validated feed; the feed itself is the completeness
    boundary (an incomplete plan yields a single error outcome, no rows).
    """
    entity = 'rumour_observations'
    if not captures or any(capture.endpoint != 'rumours' for capture in captures):
        raise core.CoreParseError('rumours:invalid_captures')
    failed = next((c for c in captures if c.status_code != 200), None)
    if failed is not None:
        return (_unavailable_capture('rumours', 'rumour_observations', failed, scope,
                                     extracted_at, universe_version),)
    newest = max(captures, key=lambda capture: (capture.captured_at, capture.capture_id))
    context = core._context(newest, scope, extracted_at, universe_version)
    try:
        pages = tuple(
            rumour_observations.extract_rumour_page(
                capture.body, competition=competition, page_number=number,
                source_page_url=capture.url, raw_capture_id=capture.capture_id,
                source_body_hash=capture.body_hash, captured_at=capture.captured_at,
                extracted_at=extracted_at, scope_id=scope.scope_id, run_id=context['run_id'],
                cycle_id=scope.cycle_id, universe_version=universe_version,
            )
            for number, capture in enumerate(captures, 1)
        )
        feed = rumour_observations.RumourFeedExtraction(pages)
    except rumours.RumourParseError as exc:
        return (core.LaneOutcome(entity, 'error', 'rumours:' + exc.reason, **context),)
    except ValueError:
        return (core.LaneOutcome(entity, 'error', 'rumours:invalid_source_data', **context),)

    by_id = {capture.capture_id: capture for capture in captures}
    if all(page.capture.status == 'observed_empty' for page in feed.pages):
        return (core.LaneOutcome(entity, 'observed_empty', 'rumours:empty_grid',
                                 proof='rumours:confirmed_empty_grid', **context),)
    outcomes = []
    for page in feed.pages:
        if page.capture.status != 'observed_present':
            continue
        rows = []
        for row in page.rows:
            try:
                row_capture = by_id[row['raw_capture_id']]
            except KeyError as exc:
                raise core.CoreParseError('rumours:row_capture_missing') from exc
            values = dict(row)
            if values['probability_percent'] is not None:
                values['probability_percent'] = repr(values['probability_percent'])
            rows.append(core._render(
                entity, values, scope, row_capture, extracted_at, context['run_id'], universe_version,
                observation_status='observed_present', status_reason='rumours:row',
                parser_revision=rumours.PARSER_VERSION, schema_revision=rumours.SCHEMA_VERSION,
            ))
        outcomes.append(core.LaneOutcome(
            entity, 'observed_present', 'rumours:page', tuple(rows),
            **core._context(by_id[page.raw_capture_id], scope, extracted_at, universe_version)))
    if not outcomes:
        return (core.LaneOutcome(entity, 'unknown', 'rumours:unresolved', **context),)
    return tuple(outcomes)


def extract_club_value_observations(
    capture: core.SourceCapture, scope: core.ScopeContext, *, extracted_at: str, universe_version: str,
) -> tuple[core.LaneOutcome, ...]:
    entity = 'club_value_observations'
    if capture.endpoint != 'club_values':
        raise core.CoreParseError('club_values:invalid_endpoint')
    if capture.status_code != 200:
        return _unavailable_capture('club_values', 'club_value_observations', capture, scope,
                                    extracted_at, universe_version)
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
    if capture.status_code != 200:
        return _unavailable_capture('achievements', 'achievement_observations', capture, scope,
                                    extracted_at, universe_version)
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
