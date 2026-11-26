"""Map the frozen stage-C section parsers into the core observation contract.

The donors interpret source pages. These adapters preserve their row verdicts
and bind each observation to the capture and run that produced it.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime
import json
import re
from urllib.parse import parse_qs, urlsplit

from bs4 import ParserRejectedMarkup

from . import (
    achievements, attendance, club_values, core, referees, representation, rumour_observations, rumours,
)
from .referee_capture import _next_directory_page, referee_directory_url


# An empty outcome has no row to carry the lane's revisions, yet its receipt
# must stay attributable: every lane outcome names them itself.
_REVISIONS = {
    'attendance_observations': (
        attendance.ATTENDANCE_PARSER_REVISION, attendance.ATTENDANCE_SCHEMA_REVISION),
    'club_value_observations': (
        club_values.CLUB_VALUE_PARSER_REVISION, club_values.CLUB_VALUE_SCHEMA_REVISION),
    'achievement_observations': (
        achievements.ACHIEVEMENT_PARSER_REVISION, str(achievements.ACHIEVEMENT_SCHEMA_REVISION)),
    'rumour_observations': (rumours.PARSER_VERSION, rumours.SCHEMA_VERSION),
    'referee_aggregate_observations': (
        referees.REFEREE_PARSER_REVISION, referees.REFEREE_SCHEMA_REVISION),
    'referee_profiles': (referees.REFEREE_PARSER_REVISION, referees.REFEREE_SCHEMA_REVISION),
    'player_representation_observations': (
        representation.REPRESENTATION_PARSER_REVISION, representation.REPRESENTATION_SCHEMA_REVISION),
}


def _outcome(entity, status, reason, rows=(), proof=None, **context) -> core.LaneOutcome:
    parser_revision, schema_revision = _REVISIONS[entity]
    return core.LaneOutcome(
        entity, status, reason, rows, proof=proof, parser_revision=parser_revision,
        schema_revision=schema_revision, **context)


def _unavailable_capture(section: str, entity: str, capture: core.SourceCapture,
                         scope: core.ScopeContext, extracted_at: str, universe_version: str):
    """A non-200 body is transport evidence, never section content."""
    context = core._context(capture, scope, extracted_at, universe_version)
    return _outcome(entity, 'unavailable', f'{section}:http_{capture.status_code}', **context)


def _distinct_captures(captures, section: str) -> None:
    """A capture id names exactly one response: URL, body and clock.

    Two pages sharing an id would let a row take the lineage of whichever
    page a lookup saw last, so contradictory evidence is refused outright.
    """
    seen = {}
    for capture in captures:
        evidence = (capture.url, capture.body_hash, capture.captured_at)
        if seen.setdefault(capture.capture_id, evidence) != evidence:
            raise core.CoreParseError(f'{section}:capture_conflict')


def _attendance_source_valid(url: str) -> bool:
    parts = urlsplit(url)
    return parts.scheme == 'https' and parts.netloc == core.TRANSFERMARKT_HOST and not parts.fragment


def _attendance_page_number(url: str) -> int:
    """Page number of a donor-validated attendance URL."""
    return int(parse_qs(urlsplit(url).query).get('page', ['1'])[0])


def _attendance_walk(captures, start_url: str, *, competition_id: str, edition_id: str):
    """Check that the supplied pages are exactly one walk from the first page.

    Returns the walk: every capture in the order the pages' own links visit
    it, each with the link its page gives to the next. Returns the reason and capture
    when the pages cannot be that walk, and None when the donor's own walk
    is the one to report.
    """
    by_url = {}
    for capture in captures:
        if not _attendance_source_valid(capture.url):
            return 'pagination_identity_invalid', capture
        url = attendance._canonical_url(capture.url)
        if url in by_url:
            return 'pagination_captures_differ', capture
        by_url[url] = capture

    walked, visited = [], set()
    capture = by_url.get(start_url, captures[0])

    def validate(url, start_path):
        return attendance._validate_attendance_url(
            url, host=core.TRANSFERMARKT_HOST, path=start_path, competition=competition_id, edition=edition_id)

    try:
        parts = urlsplit(start_url)
        if not _attendance_source_valid(start_url):
            return 'pagination_identity_invalid', capture
        # The donor reads the page number as a number: ?page=01 is its page 1.
        page = parse_qs(parts.query).get('page', ['1'])
        if len(page) != 1 or not page[0].isdigit() or int(page[0]) != 1:
            return 'pagination_start_not_first_page', capture
        current = validate(start_url, parts.path)
        while current is not None and current not in visited:
            visited.add(current)
            capture = by_url.get(current)
            if capture is None:
                return None
            if _attendance_page_number(current) != len(walked) + 1:
                return 'pagination_not_sequential', capture
            following = attendance.next_attendance_url(capture.body, capture.url)
            walked.append((capture, following))
            if following is not None and not _attendance_source_valid(following):
                return 'pagination_identity_invalid', capture
            current = validate(following, parts.path) if following is not None else None
    except attendance.AttendanceParseError as exc:
        return exc.reason, capture
    except ValueError:
        return 'pagination_identity_invalid', capture
    if visited != by_url.keys():
        return 'pagination_captures_differ', captures[-1]
    return walked


def _failing_attendance_page(walk, competition_id: str, edition_id: str):
    """The capture of the first page the donor cannot parse on its own.

    The donor reports one error for the whole walk. The page that caused it
    is the first one that fails when read alone; a page that only fails
    together with the others leaves the newest page as the evidence.
    """
    pages = list(walk)
    for page, (capture, _following) in pages:
        try:
            attendance._parse_page(page, competition=competition_id, edition=edition_id)
        except attendance.AttendanceParseError:
            return capture
    return pages[-1][1][0]


def extract_attendance_observations(
    captures: Sequence[core.SourceCapture], scope: core.ScopeContext, *, start_url: str,
    competition_id: str, edition_id: str, extracted_at: str, universe_version: str,
    final_season: bool = False, final_season_proof: str | None = None,
) -> tuple[core.LaneOutcome, ...]:
    """Observe one edition walk; the answer is one outcome, on every path."""
    entity = 'attendance_observations'
    if not captures or any(capture.endpoint != 'attendance' for capture in captures):
        raise core.CoreParseError('attendance:invalid_captures')
    failed = next((c for c in captures if c.status_code != 200), None)
    if failed is not None:
        return (_unavailable_capture('attendance', 'attendance_observations', failed, scope,
                                     extracted_at, universe_version),)
    if not isinstance(final_season, bool):
        raise core.CoreParseError('attendance:final_season_invalid')
    if final_season and not core._nonempty(final_season_proof):
        raise core.CoreParseError('attendance:final_season_proof_missing')

    context = core._context(captures[-1], scope, extracted_at, universe_version)
    _distinct_captures(captures, 'attendance')
    walk = _attendance_walk(captures, start_url, competition_id=competition_id, edition_id=edition_id)
    if not isinstance(walk, list):
        if walk is not None:
            # A terminal page offered as the start, or captures the walk never
            # visits, must not become a replacement of the whole edition.
            reason, broken = walk
            return (_outcome(entity, 'error', 'attendance:' + reason,
                             **core._context(broken, scope, extracted_at, universe_version)),)
        walk = [(capture, None) for capture in captures]  # the donor reports its own walk
    pages = []
    for capture, _following in walk:
        try:
            pages.append(attendance.AttendancePage(
                url=capture.url, body=capture.body, capture_id=capture.capture_id,
                body_hash=capture.body_hash, status_code=capture.status_code,
                # The donor's ISO format omits zero microseconds. Row lineage uses the original.
                captured_at=datetime.fromisoformat(capture.captured_at).isoformat(),
            ))
        except attendance.AttendanceParseError as exc:
            # The page the donor's own contract refuses is known here.
            return (_outcome(entity, 'error', 'attendance:' + exc.reason,
                             **core._context(capture, scope, extracted_at, universe_version)),)
    try:
        result = attendance.parse_attendance(
            pages, start_url=start_url, competition_id=competition_id, edition_id=edition_id,
        )
    except attendance.AttendanceParseError as exc:
        # The page that cannot be parsed backs the error, not a valid sibling.
        broken = _failing_attendance_page(zip(pages, walk, strict=True), competition_id, edition_id)
        return (_outcome(entity, 'error', 'attendance:' + exc.reason,
                         **core._context(broken, scope, extracted_at, universe_version)),)
    except (ValueError, OverflowError):
        return (_outcome(entity, 'error', 'attendance:invalid_source_data', **context),)

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
        try:
            rows.append(core._render(
                entity, values, scope, capture=row_capture, run_id=context['run_id'],
                extracted_at=extracted_at, universe_version=universe_version,
                observation_status=row.observation_status,
                status_reason='attendance:' + row.status_reason,
                parser_revision=attendance.ATTENDANCE_PARSER_REVISION,
                schema_revision=attendance.ATTENDANCE_SCHEMA_REVISION,
            ))
        except core.CoreParseError as exc:
            # A source value the row cannot hold is this section's error,
            # backed by the page that carries it: the run goes on.
            return (_outcome(entity, 'error', 'attendance:' + exc.reason,
                             **core._context(row_capture, scope, extracted_at, universe_version)),)
    if rows:
        # The outcome's capture is the newest page that CONTRIBUTED a row: a
        # newer but rowless trailing page must not shift the plan's clock.
        contributing = {row['source_capture_id'] for row in rows}
        context['capture'] = max(
            (capture for capture in captures if capture.capture_id in contributing),
            key=lambda capture: (capture.captured_at, capture.capture_id))
    # Every walked page keeps its own lineage, the rowless ones included, in
    # the order the walk visits it, each naming the page its own link leads
    # to: the manifest is the walk, not only its start.
    pages = tuple(
        (capture.url, capture.capture_id, capture.captured_at, capture.body_hash,
         walk[index + 1][0].url if index + 1 < len(walk)
         else '' if following is None else attendance._canonical_url(following))
        for index, (capture, following) in enumerate(walk))
    return (_outcome(entity, result.status, reason, tuple(rows), pages=pages, **context),)


def parse_json_integer(token: str) -> int | str:
    """Keep an oversized integer token available for field-level validation."""
    try:
        return int(token)
    except ValueError:
        return token


def _representation_packet_rows(payload, expected_ids):
    if not isinstance(payload, Mapping) or payload.get('success') is not True:
        raise ValueError('invalid_packet')
    data = payload.get('data')
    if not isinstance(data, list) or not 1 <= len(data) <= 100 or len(data) != len(expected_ids):
        raise ValueError('invalid_packet')
    rows = {}
    for row in data:
        if not isinstance(row, Mapping):
            raise ValueError('invalid_packet')
        player_id = representation.canonical_player_id(row.get('id'))
        if player_id in rows:
            raise ValueError('invalid_packet')
        rows[player_id] = row
    if rows.keys() != set(expected_ids):
        raise ValueError('invalid_packet')
    return rows


def extract_representation_observations(
    capture: core.SourceCapture, scope: core.ScopeContext, *, expected_ids: Sequence[str],
    extracted_at: str, universe_version: str,
) -> tuple[core.LaneOutcome, ...]:
    entity = 'player_representation_observations'
    if capture.endpoint != 'representation':
        raise core.CoreParseError('representation:invalid_endpoint')
    if capture.status_code != 200:
        return (_unavailable_capture('representation', 'player_representation_observations', capture, scope,
                                     extracted_at, universe_version),)
    context = core._context(capture, scope, extracted_at, universe_version)
    requested = core._representation_query_ids(urlsplit(capture.url).query)
    if not requested or sorted(requested) != sorted(expected_ids):
        # The packet must answer the whole request its URL made: a narrowed
        # expectation would certify a capture that never asked for the rest.
        return (_outcome(entity, 'error', 'representation:requested_ids_differ', **context),)

    def packet_error(reason):
        return (core.captureless_outcome(
            entity, scope, status='error', reason='representation:' + reason,
            extracted_at=extracted_at, run_id=context['run_id'], universe_version=universe_version,
            subject=core.canonical_json({'player_ids': list(expected_ids)}).decode(),
        ),)

    try:
        payload = json.loads(capture.text, parse_int=parse_json_integer)
    except (ValueError, RecursionError):
        return packet_error('invalid_json')
    try:
        rows = _representation_packet_rows(payload, expected_ids)
    except ValueError:
        return packet_error('invalid_packet')

    outcomes = []
    for player_id in sorted(rows, key=int):
        core._check_scope_against_capture(entity, {'player_id': player_id}, capture)
        player_context = context | {'scope': replace(scope, player_id=player_id)}
        try:
            assertion = representation.parse_representation_packet(
                {**payload, 'data': [rows[player_id]]}, expected_ids=(player_id,))[player_id]
        except ValueError as exc:
            reason = 'representation:' + getattr(exc, 'reason', 'invalid_packet')
            outcomes.append(_outcome(entity, 'error', reason, **player_context))
            continue
        status = assertion.observation_status
        row_reason = 'representation:' + assertion.status_reason
        if status == 'error':
            outcomes.append(_outcome(entity, status, row_reason, **player_context))
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
        try:
            row = core._render(
                entity, values, observation_status=status, status_reason=row_reason,
                parser_revision=representation.REPRESENTATION_PARSER_REVISION,
                schema_revision=representation.REPRESENTATION_SCHEMA_REVISION, **player_context,
            )
        except core.CoreParseError as exc:
            # A row the core cannot render is this player's error; the other
            # players of the packet are observed all the same.
            outcomes.append(_outcome(entity, 'error', 'representation:' + exc.reason, **player_context))
            continue
        outcomes.append(_outcome(entity, status, reason, (row,), **player_context))
    return tuple(outcomes)


_DIRECTORY_PAGE = re.compile(
    r'/(?P<slug>[A-Za-z0-9_-]+)/schiedsrichter/(?P<route>wettbewerb|pokalwettbewerb)/'
    r'(?P<competition_id>[A-Za-z0-9]+)/saison_id/(?P<edition_id>[0-9]{4})'
    r'(?:/plus/1)?(?:/page/(?P<page>[0-9]+))?'
)


def _referee_walk_failure(captures):
    """Find the page that breaks the supplied directory walk, if any.

    The pages must be one contiguous walk from page 1 that only a terminal
    page closes: a trailing next link means pages are missing and nothing of
    the directory is certified. Every page URL is the donor's route, with
    its page in the path: a query or fragment is not part of it. Returns the
    page that breaks the walk, its capture and the reason.
    """
    scope = identity = None
    for index, capture in enumerate(captures, 1):
        url = urlsplit(capture.url)
        match = _DIRECTORY_PAGE.fullmatch(url.path)
        if (match is None or url.netloc != core.TRANSFERMARKT_HOST
                or url.query or url.fragment):
            return capture, 'directory_identity_invalid'
        if scope is None:
            scope = match.groupdict()
            identity = (match['route'], match['competition_id'], match['edition_id'])
        # Every page continues the very directory the first page started.
        if (match['route'], match['competition_id'], match['edition_id']) != identity:
            return capture, 'directory_identity_invalid'
        if int(match['page'] or '1') != index:
            return capture, 'directory_pagination_incomplete'
    del scope['page']
    for index, capture in enumerate(captures, 1):
        try:
            has_next = _next_directory_page(capture.text, scope, index)
        except UnicodeDecodeError:
            return capture, 'invalid_utf8'
        except ValueError as exc:
            # RefereeCaptureError carries its reason; the URL parser of a
            # malformed link raises a plain ValueError.
            return capture, getattr(exc, 'reason', None) or 'pagination_link_invalid'
        if has_next != (index < len(captures)):
            return capture, 'directory_pagination_incomplete'
        if has_next and captures[index].url != referee_directory_url(scope, page=index + 1):
            # The next capture must be the very page this link names.
            return captures[index], 'directory_pagination_incomplete'
    return None


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
    _distinct_captures(captures, 'referees')
    failure = _referee_walk_failure(captures)
    if failure is not None:
        # The walk is not certified whole: the directory keeps no partial rows
        # and the failing page itself backs the error. The identity of the
        # directory is read only once its first page proved well-formed.
        broken, reason = failure
        return (_outcome(entity, 'error', 'referees:' + reason,
                         **core._context(broken, scope, extracted_at, universe_version)),)
    competition_id, edition_id = core._referee_directory_identity(urlsplit(captures[0].url).path)
    directory = {'competition_id': competition_id, 'edition_id': edition_id}

    newest = max(captures, key=lambda capture: (capture.captured_at, capture.capture_id))
    context = core._context(newest, scope, extracted_at, universe_version)
    first_page = {}
    reading = None  # the capture whose page the donor is reading

    def failed(reason: str):
        # The page being read when the donor failed backs the error; a
        # failure of the reconciled directory is backed by its newest page.
        backing = newest if reading is None else reading
        return _outcome(entity, 'error', 'referees:' + reason,
                        **core._context(backing, scope, extracted_at, universe_version))

    try:
        pages = [capture.text for capture in captures]
        for index, html in enumerate(pages):
            reading = captures[index]
            page = referees.parse_referee_directory_page(html, **directory)
            for aggregate in page.aggregates:
                first_page.setdefault(aggregate.natural_key, index)
        reading = None
        # Every supplied page is accounted for by the validated walk above.
        result = referees.reconcile_referee_directory_pages(
            pages, expected_page_count=len(pages), **directory,
        )
    except referees.RefereeParseError as exc:
        return (failed(exc.reason),)
    except ValueError as exc:
        return (failed(str(exc) or 'invalid_source_data'),)

    if not result.aggregates:
        if result.completeness_proof:
            proof = 'referees:' + result.completeness_proof
            return (_outcome(entity, 'observed_empty', proof, proof=proof, **context),)
        return (_outcome(entity, 'error', 'referees:completeness_proof_missing', **context),)

    rows_by_page = [[] for _ in captures]
    try:
        for aggregate in result.aggregates:
            index = first_page[aggregate.natural_key]
            capture = captures[index]
            values = aggregate.as_dict()
            values['observed_at'] = capture.captured_at
            rows_by_page[index].append(core._render(
                entity, values, scope, capture, extracted_at, context['run_id'], universe_version,
                observation_status=aggregate.observation_status,
                status_reason='referees:' + aggregate.status_reason,
                parser_revision=referees.REFEREE_PARSER_REVISION,
                schema_revision=referees.REFEREE_SCHEMA_REVISION,
            ))
    except core.CoreParseError as exc:
        # The directory keeps no partial rows: the page of the row backs the error.
        return (_outcome(entity, 'error', 'referees:' + exc.reason,
                         **core._context(capture, scope, extracted_at, universe_version)),)
    return tuple(
        _outcome(entity, 'observed_present', 'referees:page', tuple(rows),
                         **core._context(capture, scope, extracted_at, universe_version))
        for capture, rows in zip(captures, rows_by_page) if rows
    )


def extract_referee_profile_observations(
    capture: core.SourceCapture, scope: core.ScopeContext, *, referee_id: str,
    extracted_at: str, universe_version: str,
) -> tuple[core.LaneOutcome, ...]:
    """Observe one referee profile; the answer is one outcome, on every path."""
    entity = 'referee_profiles'
    if capture.endpoint != 'referee_profile':
        raise core.CoreParseError('referees:invalid_endpoint')
    if capture.status_code != 200:
        return (_unavailable_capture('referees', 'referee_profiles', capture, scope,
                                     extracted_at, universe_version),)
    context = core._context(capture, scope, extracted_at, universe_version)
    try:
        core._check_scope_against_capture(entity, {'referee_id': referee_id}, capture)
    except core.CoreParseError as exc:
        return (_outcome(entity, 'error', 'referees:' + exc.reason, **context),)
    try:
        profile = referees.parse_referee_profile(capture.text, referee_id=referee_id)
    except referees.RefereeParseError as exc:
        return (_outcome(entity, 'error', 'referees:' + exc.reason, **context),)
    except UnicodeDecodeError:
        return (_outcome(entity, 'error', 'referees:invalid_utf8', **context),)

    reason = 'referees:profile'
    values = profile.as_dict()
    values['observed_at'] = capture.captured_at
    try:
        row = core._render(
            entity, values, observation_status='observed_present', status_reason=reason,
            parser_revision=referees.REFEREE_PARSER_REVISION,
            schema_revision=referees.REFEREE_SCHEMA_REVISION, **context,
        )
    except core.CoreParseError as exc:
        # A row the core cannot render is this profile's error.
        return (_outcome(entity, 'error', 'referees:' + exc.reason, **context),)
    return (_outcome(entity, 'observed_present', reason, (row,), **context),)


def _body_marker_row(table) -> bool:
    """The donor's "no entries" row: the body's only row, one marked cell.

    A marked cell anywhere else, a header cell included, is not that row.
    """
    body = table.find('tbody', recursive=False)
    if body is None:
        return False
    rows = body.find_all('tr', recursive=False)
    if len(rows) != 1:
        return False
    cells = rows[0].find_all('td', recursive=False)
    return len(cells) == 1 and 'no-entries' in cells[0].get('class', [])


def extract_rumour_observations(
    captures: Sequence[core.SourceCapture], scope: core.ScopeContext, *,
    competition: rumours.CompetitionContext, extracted_at: str, universe_version: str,
    failure: str | None = None, broken: core.SourceCapture | None = None,
) -> tuple[core.LaneOutcome, ...]:
    """Observe one COMPLETE rumour feed, one write-plan outcome per page.

    A merge plan owns exactly one capture, so each page's rows become their own
    outcome inside the one validated feed; the feed itself is the completeness
    boundary (an incomplete plan yields a single error outcome, no rows). A
    confirmed empty page is one proof-bearing outcome: its capture stays in the
    evidence bundle instead of vanishing between its populated neighbours.
    """
    entity = 'rumour_observations'
    if not captures or any(capture.endpoint != 'rumours' for capture in captures):
        raise core.CoreParseError('rumours:invalid_captures')
    failed = next((c for c in captures if c.status_code != 200), None)
    if failed is not None:
        return (_unavailable_capture('rumours', 'rumour_observations', failed, scope,
                                     extracted_at, universe_version),)
    newest = broken or max(captures, key=lambda capture: (capture.captured_at, capture.capture_id))
    context = core._context(newest, scope, extracted_at, universe_version)
    _distinct_captures(captures, 'rumours')
    if failure is not None:
        # The walk stopped on conflicting or unreadable evidence of its own:
        # the pages it did fetch are not the feed. The page that broke it,
        # when there is one, backs the error.
        return (_outcome(entity, 'error', 'rumours:' + failure, **context),)
    # The page being read backs an error: the newest page of the feed is
    # not necessarily the one that could not be parsed.
    current = newest
    try:
        # The supplied pages must form the complete plan their pagers evidence
        # before any page of the feed is interpreted.
        paginations = []
        for current in captures:
            paginations.append(rumours.parse_rumour_pagination(
                current.text, competition=competition, page_url=current.url))
        if [capture.url for capture in captures] != list(rumours.build_rumour_page_plan(paginations)):
            raise rumours.RumourParseError('incomplete_page_plan')
        pages = []
        for number, current in enumerate(captures, 1):
            pages.append(rumour_observations.extract_rumour_page(
                current.body, competition=competition, page_number=number,
                source_page_url=current.url, raw_capture_id=current.capture_id,
                source_body_hash=current.body_hash, captured_at=current.captured_at,
                extracted_at=extracted_at, scope_id=scope.scope_id, run_id=context['run_id'],
                cycle_id=scope.cycle_id, universe_version=universe_version,
            ))
        feed = rumour_observations.RumourFeedExtraction(pages)
    except rumours.RumourParseError as exc:
        failed = core._context(current, scope, extracted_at, universe_version)
        return (_outcome(entity, 'error', 'rumours:' + exc.reason, **failed),)
    except ValueError:
        failed = core._context(current, scope, extracted_at, universe_version)
        return (_outcome(entity, 'error', 'rumours:invalid_source_data', **failed),)

    outcomes = []
    # The feed sorts its pages by page number, which is the capture position.
    # Every row of a page binds to that page's own capture.
    for page, capture in zip(feed.pages, captures, strict=True):
        page_context = core._context(capture, scope, extracted_at, universe_version)
        if page.capture.status == 'observed_empty':
            soup = rumours._soup(capture.text)
            if rumours._confirmed_empty_grid(soup):
                proof, reason = 'rumours:confirmed_empty_grid', 'rumours:empty_grid'
            else:
                # Only the rumour table's body can carry the donor's marker row.
                table = rumours._rumour_table(soup)
                proof = reason = ('rumours:no_entries_row' if _body_marker_row(table)
                                  else 'rumours:empty_table_body')
            outcomes.append(_outcome(entity, 'observed_empty', reason, proof=proof, **page_context))
            continue
        rows = []
        try:
            for row in page.rows:
                if row['raw_capture_id'] != capture.capture_id:
                    raise core.CoreParseError('rumours:row_capture_missing')
                values = dict(row)
                if values['probability_percent'] is not None:
                    values['probability_percent'] = repr(values['probability_percent'])
                rows.append(core._render(
                    entity, values, scope, capture, extracted_at, context['run_id'], universe_version,
                    observation_status='observed_present', status_reason='rumours:row',
                    parser_revision=rumours.PARSER_VERSION, schema_revision=rumours.SCHEMA_VERSION,
                ))
        except core.CoreParseError as exc:
            # A row of the feed no page can hold is the feed's error, backed
            # by the page it came from: no page of the feed is certified.
            return (_outcome(entity, 'error', 'rumours:' + exc.reason, **page_context),)
        outcomes.append(_outcome(entity, 'observed_present', 'rumours:page', tuple(rows), **page_context))
    return tuple(outcomes)


def extract_club_value_observations(
    capture: core.SourceCapture, scope: core.ScopeContext, *, competition_id: str,
    extracted_at: str, universe_version: str,
) -> tuple[core.LaneOutcome, ...]:
    """Observe one club-values page of ``competition_id``.

    The page belongs to the target's competition, which follows the donor's
    own id grammar (``U-21`` is one); the enclosing scope keeps its league.
    """
    entity = 'club_value_observations'
    if capture.endpoint != 'club_values':
        raise core.CoreParseError('club_values:invalid_endpoint')
    if capture.status_code != 200:
        return (_unavailable_capture('club_values', 'club_value_observations', capture, scope,
                                     extracted_at, universe_version),)
    context = core._context(capture, scope, extracted_at, universe_version)
    try:
        club_capture = club_values.ClubValueCapture(
            competition_id=competition_id, source_url=capture.url, raw_capture_id=capture.capture_id,
            source_body_hash=capture.body_hash, captured_at=capture.captured_at,
            extracted_at=extracted_at, scope_id=scope.scope_id, run_id=context['run_id'],
            cycle_id=scope.cycle_id, universe_version=universe_version,
        )
        extraction = club_values.parse_club_values(capture.text, club_capture)
    except club_values.ClubValueParseError as exc:
        return (_outcome(entity, 'error', 'club_values:' + exc.reason, **context),)
    except UnicodeDecodeError:
        return (_outcome(entity, 'error', 'club_values:invalid_utf8', **context),)
    except (ValueError, TypeError, ArithmeticError, RecursionError):
        return (_outcome(entity, 'error', 'club_values:invalid_source_data', **context),)

    by_club = {}
    try:
        for point in extraction.rows:
            row = core._render(
                entity, point, observation_status=point['observation_status'],
                status_reason='club_values:point',
                parser_revision=club_values.CLUB_VALUE_PARSER_REVISION,
                schema_revision=club_values.CLUB_VALUE_SCHEMA_REVISION, **context,
            )
            by_club.setdefault(point['club_id'], []).append(row)
    except core.CoreParseError as exc:
        # A value a row cannot hold is this section's error; the run goes on.
        return (_outcome(entity, 'error', 'club_values:' + exc.reason, **context),)

    outcomes = []
    for rows in by_club.values():
        statuses = {row['observation_status'] for row in rows}
        if 'observed_present' in statuses:
            status, reason = 'observed_present', 'club_rows_present'
        elif statuses == {'explicit_none'}:
            status, reason = 'explicit_none', 'club_rows_explicit_none'
        else:
            status, reason = 'unknown', 'club_rows_unresolved'
        outcomes.append(_outcome(entity, status, 'club_values:' + reason, tuple(rows), **context))
    return tuple(outcomes)


def extract_achievement_observations(
    capture: core.SourceCapture, scope: core.ScopeContext, *, recipient_type: str, recipient_id: str,
    extracted_at: str, universe_version: str,
) -> tuple[core.LaneOutcome, ...]:
    """Observe one holder's achievements; the answer is one outcome, on every path."""
    entity = 'achievement_observations'
    if capture.endpoint != f'{recipient_type}_achievement':
        raise core.CoreParseError('achievements:invalid_endpoint')
    if capture.status_code != 200:
        return (_unavailable_capture('achievements', 'achievement_observations', capture, scope,
                                     extracted_at, universe_version),)
    core._check_scope_against_capture(
        entity, {'recipient_type': recipient_type, 'recipient_id': recipient_id}, capture,
    )
    context = core._context(capture, scope, extracted_at, universe_version)
    try:
        source = capture.text if recipient_type == 'coach' else json.loads(capture.text)
    except (ValueError, RecursionError):
        reason = 'achievements:invalid_utf8' if recipient_type == 'coach' else 'achievements:invalid_json'
        return (_outcome(entity, 'error', reason, **context),)
    try:
        result = achievements.parse_achievements(source, recipient_type=recipient_type, recipient_id=recipient_id)
    except achievements.AchievementParseError as exc:
        return (_outcome(entity, 'error', 'achievements:' + exc.reason, **context),)
    except (ParserRejectedMarkup, ValueError, RecursionError):
        return (_outcome(entity, 'error', 'achievements:invalid_source_data', **context),)

    if result.status == 'observed_empty':
        return (_outcome(entity, result.status, 'achievements:empty_collection',
                         proof=f'achievements:empty_collection:{recipient_type}', **context),)

    reason = 'achievements:row'
    rows = []
    try:
        for achievement in result.rows:
            values = achievement.as_dict()
            if values['recipient_id'] != recipient_id or values['recipient_type'] != recipient_type:
                raise core.CoreParseError('row_scope_mismatch')
            rows.append(core._render(
                entity, values, observation_status='observed_present', status_reason=reason,
                parser_revision=achievements.ACHIEVEMENT_PARSER_REVISION,
                schema_revision=str(achievements.ACHIEVEMENT_SCHEMA_REVISION), **context,
            ))
    except core.CoreParseError as exc:
        # A row the core cannot render (or one that names another holder) is
        # this holder's error; the run goes on.
        return (_outcome(entity, 'error', 'achievements:' + exc.reason, **context),)
    return (_outcome(entity, result.status, reason, tuple(rows), **context),)
