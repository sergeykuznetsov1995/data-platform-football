"""Raw-first capture of nonmatch Transfermarkt referee pages."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import re
from typing import Any, Iterable, Mapping
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from scrapers.transfermarkt.models import FetchStatus
from scrapers.transfermarkt.referees import (
    REFEREE_AGGREGATE_COLUMNS,
    REFEREE_PARSER_REVISION,
    REFEREE_SCHEMA_REVISION,
    RefereeDirectoryEntry,
    parse_referee_directory_page,
    parse_referee_profile,
    reconcile_referee_directory_pages,
)

REFEREE_PROFILE_TABLE = 'iceberg.bronze.transfermarkt_referee_profiles'
REFEREE_AGGREGATE_TABLE = 'iceberg.bronze.transfermarkt_referee_season_aggregates'
REFEREE_RECEIPTS_TABLE = 'iceberg.ops.transfermarkt_referee_receipts_v1'
_LINEAGE_COLUMNS = (
    'source_competition_id', 'source_edition_id', 'source_url', 'raw_capture_id',
    'source_body_hash', 'source_endpoint', 'scope_id', 'cycle_id', 'captured_at',
    'extracted_at', 'parser_revision', 'schema_revision', 'run_id', 'universe_version',
)
_METADATA_COLUMNS = ('_source', '_entity_type', '_batch_id', '_ingested_at')
REFEREE_PROFILE_COLUMNS = (
    'referee_id', 'name', 'nationality', 'birth_date', 'observed_at',
    *_LINEAGE_COLUMNS, *_METADATA_COLUMNS,
)
REFEREE_AGGREGATE_PHYSICAL_COLUMNS = (
    *REFEREE_AGGREGATE_COLUMNS[:-2], 'source_row', 'source_column', 'observed_at',
    *_LINEAGE_COLUMNS, *_METADATA_COLUMNS,
)
_SLUG_RE = re.compile(r'[^/]+')
_COMPETITION_PATH_RE = re.compile(
    r'/startseite/(?P<route>wettbewerb|pokalwettbewerb)/(?P<id>[A-Za-z0-9]+)(?:[/?#]|$)',
)


class RefereeCaptureError(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class RefereeCaptureBundle:
    scope_id: str
    competition_id: str
    edition_id: str
    capture_kind: str
    run_id: str
    universe_version: str
    extracted_at: str
    profiles: tuple[dict[str, Any], ...] = ()
    aggregates: tuple[dict[str, Any], ...] = ()
    directory_referees: tuple[RefereeDirectoryEntry, ...] = ()
    expected_directory_pages: int = 0

    def __post_init__(self):
        for name in ('scope_id', 'competition_id', 'edition_id', 'run_id', 'universe_version'):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise RefereeCaptureError(f'{name}_invalid')
        if self.capture_kind not in {'aggregates', 'profiles'}:
            raise RefereeCaptureError('capture_kind_invalid')
        object.__setattr__(self, 'extracted_at', _timestamp(self.extracted_at))
        if self.capture_kind == 'aggregates' and (not self.aggregates or self.expected_directory_pages < 1):
            raise RefereeCaptureError('aggregate_capture_incomplete')
        if self.capture_kind == 'profiles' and (not self.profiles or self.expected_directory_pages):
            raise RefereeCaptureError('profile_capture_invalid')

    @property
    def raw_capture_ids(self) -> tuple[str, ...]:
        return tuple(sorted({
            row['raw_capture_id'] for row in (*self.profiles, *self.aggregates)
        }))


def referee_directory_url(scope: Mapping[str, Any], *, page: int = 1) -> str:
    if isinstance(page, bool) or not isinstance(page, int) or page < 1:
        raise RefereeCaptureError('directory_page_invalid')
    competition = str(scope.get('competition_id') or '')
    edition = str(scope.get('edition_id') or '')
    slug = str(scope.get('slug') or '')
    route = str(scope.get('route') or '')
    if not _SLUG_RE.fullmatch(slug) or route not in {'wettbewerb', 'pokalwettbewerb'}:
        raise RefereeCaptureError('directory_identity_invalid')
    if not re.fullmatch(r'[A-Za-z0-9]+', competition) or not re.fullmatch(r'[0-9]{4}', edition):
        raise RefereeCaptureError('directory_identity_invalid')
    url = (
        f'https://www.transfermarkt.com/{slug}/schiedsrichter/{route}/'
        f'{competition}/saison_id/{edition}/plus/1'
    )
    return url if page == 1 else f'{url}/page/{page}'


def referee_profile_url(referee_id: Any) -> str:
    if isinstance(referee_id, bool) or not re.fullmatch(r'[0-9]+', str(referee_id)):
        raise RefereeCaptureError('referee_id_invalid')
    number = int(referee_id)
    if number < 1:
        raise RefereeCaptureError('referee_id_invalid')
    return f'https://www.transfermarkt.com/-/profil/schiedsrichter/{number}'


def referee_scopes(
    registry_rows: Iterable[Mapping[str, Any]], *, historical: bool = False,
) -> tuple[dict[str, Any], ...]:
    from dags.utils.transfermarkt_scope_planner import eligible_registry_scopes

    rows = list(registry_rows)
    targets = eligible_registry_scopes(rows)
    by_key = {
        (str(row.get('competition_id')), str(row.get('edition_id'))): row for row in rows
    }
    selected = []
    for target in targets:
        if target.current == historical:
            continue
        row = by_key.get((target.competition_id, target.edition_id))
        if row is None:
            raise RefereeCaptureError('registry_scope_missing')
        source_url = str(row.get('competition_source_url') or '')
        match = _COMPETITION_PATH_RE.search(urlsplit(source_url).path)
        if match is None or match.group('id').upper() != target.competition_id.upper():
            raise RefereeCaptureError('registry_route_invalid')
        selected.append({
            'scope_id': target.scope_id,
            'competition_id': target.competition_id,
            'edition_id': target.edition_id,
            'current': target.current,
            'slug': str(row.get('slug') or ''),
            'route': match.group('route'),
            'registry_snapshot_id': str(row.get('registry_snapshot_id') or ''),
            'canonical_season': str(row.get('canonical_season') or ''),
            'competition_source_url': source_url,
            '_registry_row': row,
        })
    return tuple(selected)


def capture_referee_directory(
    client: Any, scope: Mapping[str, Any], *, run_id: str, universe_version: str,
    extracted_at: Any,
) -> RefereeCaptureBundle:
    extracted = _timestamp(extracted_at)
    pages = []
    evidence_by_key = {}
    raw_ids = []
    page = 1
    while True:
        if page > 100:
            raise RefereeCaptureError('directory_pagination_incomplete')
        url = referee_directory_url(scope, page=page)
        outcome = client.fetch(url, as_json=False, label='referee_directory')
        evidence = _capture_evidence(client, outcome, url, 'referee_directory', scope)
        pages.append(outcome.value)
        raw_ids.append(evidence['raw_capture_id'])
        parsed = parse_referee_directory_page(
            outcome.value,
            competition_id=scope['competition_id'], edition_id=scope['edition_id'],
        )
        for row in parsed.aggregates:
            evidence_by_key.setdefault(row.natural_key, evidence)
        if not _next_directory_page(outcome.value, scope, page):
            break
        page += 1
    result = reconcile_referee_directory_pages(
        pages,
        competition_id=scope['competition_id'], edition_id=scope['edition_id'],
        expected_page_count=len(pages),
    )
    rows = []
    for aggregate in result.aggregates:
        evidence = evidence_by_key[aggregate.natural_key]
        rows.append(_lineage_row(
            aggregate.as_dict(), evidence, scope, run_id=run_id,
            universe_version=universe_version, extracted_at=extracted,
            entity_type='referee_season_aggregate',
        ))
    return RefereeCaptureBundle(
        str(scope['scope_id']), str(scope['competition_id']), str(scope['edition_id']),
        'aggregates', run_id, universe_version, extracted,
        aggregates=tuple(rows), directory_referees=result.referees,
        expected_directory_pages=len(pages),
    )


def capture_referee_profiles(
    client: Any, referee_ids: Iterable[Any], *, scope: Mapping[str, Any],
    run_id: str, universe_version: str, extracted_at: Any,
) -> RefereeCaptureBundle:
    extracted = _timestamp(extracted_at)
    ids = sorted({str(int(str(value))) for value in referee_ids}, key=int)
    if not ids:
        raise RefereeCaptureError('profile_denominator_empty')
    profiles = []
    for referee_id in ids:
        url = referee_profile_url(referee_id)
        outcome = client.fetch(url, as_json=False, label='referee_profile')
        evidence = _capture_evidence(client, outcome, url, 'referee_profile', scope)
        profile = parse_referee_profile(outcome.value, referee_id=referee_id)
        profiles.append(_lineage_row(
            profile.as_dict(), evidence, scope, run_id=run_id,
            universe_version=universe_version, extracted_at=extracted,
            entity_type='referee_profile',
        ))
    return RefereeCaptureBundle(
        str(scope['scope_id']), str(scope['competition_id']), str(scope['edition_id']),
        'profiles', run_id, universe_version, extracted, profiles=tuple(profiles),
    )


def _capture_evidence(client, outcome, url: str, endpoint: str, scope: Mapping[str, Any]):
    if outcome.status != FetchStatus.OK or not isinstance(outcome.value, str) or not outcome.value.strip():
        raise RefereeCaptureError('capture_unavailable')
    if not outcome.raw_capture_id or client._raw_store is None:
        raise RefereeCaptureError('raw_lineage_missing')
    body, raw = client._raw_store.load_capture(outcome.raw_capture_id)
    if (
        raw.url != url or raw.endpoint != endpoint or raw.status_code != 200
        or raw.scope_id != scope['scope_id'] or raw.content_hash != outcome.raw_body_hash
        or raw.fetched_at != outcome.raw_fetched_at
        or hashlib.sha256(body).hexdigest() != raw.content_hash
        or body.decode('utf-8') != outcome.value
    ):
        raise RefereeCaptureError('raw_lineage_mismatch')
    return {
        'source_url': url,
        'raw_capture_id': raw.capture_id,
        'source_body_hash': raw.content_hash,
        'source_endpoint': endpoint,
        'cycle_id': raw.cycle_id,
        'captured_at': raw.fetched_at,
        'observed_at': raw.fetched_at,
    }


def _lineage_row(
    values: Mapping[str, Any], evidence: Mapping[str, Any], scope: Mapping[str, Any],
    *, run_id: str, universe_version: str, extracted_at: str, entity_type: str,
) -> dict[str, Any]:
    return {
        **values,
        **evidence,
        'source_competition_id': str(scope['competition_id']),
        'source_edition_id': str(scope['edition_id']),
        'scope_id': str(scope['scope_id']),
        'run_id': run_id,
        'universe_version': universe_version,
        'extracted_at': extracted_at,
        'parser_revision': REFEREE_PARSER_REVISION,
        'schema_revision': REFEREE_SCHEMA_REVISION,
        '_source': 'transfermarkt',
        '_entity_type': entity_type,
        '_ingested_at': extracted_at,
    }


def _next_directory_page(html: str, scope: Mapping[str, Any], page: int) -> bool:
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.select('.naechste-seite a[href], a.naechste-seite[href]')
    if not links:
        return False
    if len(links) != 1:
        raise RefereeCaptureError('directory_pagination_ambiguous')
    target = urljoin(referee_directory_url(scope, page=page), links[0]['href'])
    if '/spielbericht' in urlsplit(target).path:
        raise RefereeCaptureError('route_forbidden')
    if target != referee_directory_url(scope, page=page + 1):
        raise RefereeCaptureError('pagination_link_invalid')
    return True


def _timestamp(value: Any) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    except ValueError as exc:
        raise RefereeCaptureError('timestamp_invalid') from exc
    if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RefereeCaptureError('timestamp_invalid')
    return parsed.astimezone(timezone.utc).isoformat()


__all__ = [
    'REFEREE_AGGREGATE_PHYSICAL_COLUMNS', 'REFEREE_AGGREGATE_TABLE',
    'REFEREE_PROFILE_COLUMNS', 'REFEREE_PROFILE_TABLE', 'REFEREE_RECEIPTS_TABLE',
    'RefereeCaptureBundle', 'RefereeCaptureError', 'capture_referee_directory',
    'capture_referee_profiles', 'referee_directory_url', 'referee_profile_url',
    'referee_scopes',
]
