"""Selective current captures that reconstruct a complete edition roster.

No writer or network calls live here. A retained club needs a committed Bronze
manifest; a freshly captured club needs complete parsed fields and raw lineage.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from types import MappingProxyType
from typing import Any, Iterable, Mapping
from urllib.parse import urlsplit

from scrapers.transfermarkt.registry import deterministic_scope_id


FULL_PLAYER_FIELDS = frozenset({
    'player_id', 'player_slug', 'name', 'club_id', 'market_value_eur',
    'position', 'dob', 'age', 'height_cm', 'foot', 'nationality', 'contract_until',
})
_CLUB_LINK = re.compile(r'^/([^/]+)/(?:startseite|kader|spielplan)/verein/(\d+)(?:/|$)')
_BIO_FIELDS = ('position', 'dob', 'age', 'height_cm', 'foot', 'nationality', 'contract_until')


class CurrentCaptureError(ValueError):
    """Selective data would lose full-squad fields, members or source proof."""


def _text(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CurrentCaptureError(f'{name} must be nonempty text')
    return value


def _id(name: str, value: Any) -> str:
    if isinstance(value, bool) or not re.fullmatch(r'\d+', str(value)):
        raise CurrentCaptureError(f'{name} must be a numeric source ID')
    return str(value)


def _timestamp(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00')) if isinstance(value, str) else value
    except ValueError as exc:
        raise CurrentCaptureError('invalid fetched timestamp') from exc
    if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CurrentCaptureError('raw_fetched_at requires a timezone')
    return parsed.astimezone(timezone.utc)


def _hash(name: str, value: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value):
        raise CurrentCaptureError(f'{name} must be lowercase sha256')


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False, default=lambda v: v.isoformat() if isinstance(v, (date, datetime)) else str(v))


def semantic_signature(value: Any) -> str:
    """Only normalized semantic values enter the content address."""
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def _normal(value: str) -> str:
    return re.sub(r'\s+', ' ', value.replace('\xa0', ' ')).strip()


def club_listing_signatures(html: str) -> dict[str, dict[str, str]]:
    """Fingerprint source club rows, ignoring scripts, tokens and URL decoration."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table', class_='items')
    if table is None:
        raise CurrentCaptureError('club listing has no items table')
    for decorative in table.select('script, style, input, .sort-arrow, .sort-icon'):
        decorative.decompose()
    headers = [_normal(th.get_text(' ', strip=True)).lower()
               for th in table.select('thead th')]
    result: dict[str, dict[str, str]] = {}
    body = table.find('tbody') or table
    for tr in body.find_all('tr', recursive=False):
        links = []
        for anchor in tr.find_all('a', href=True):
            match = _CLUB_LINK.match(urlsplit(anchor['href']).path)
            if match:
                links.append((anchor, match))
        if not links:
            continue
        club_ids = {match.group(2) for _, match in links}
        if len(club_ids) != 1:
            raise CurrentCaptureError('listing row contains multiple club identities')
        anchor, match = max(links, key=lambda item: len(item[0].get_text(' ', strip=True)))
        name = _normal(anchor.get_text(' ', strip=True) or anchor.get('title', ''))
        if not name:
            raise CurrentCaptureError('listing club has no name')
        cells = tr.find_all('td', recursive=False)
        content = {
            f'{index}:{headers[index] if index < len(headers) else "column"}':
            _normal(cell.get_text(' ', strip=True))
            for index, cell in enumerate(cells)
        }
        item = {'signature': semantic_signature({'club_id': match.group(2), 'cells': content}),
                'club_slug': match.group(1), 'club_name': name}
        existing = result.get(match.group(2))
        if existing is not None and existing != item:
            raise CurrentCaptureError('conflicting duplicate club listing')
        result[match.group(2)] = item
    return result


@dataclass(frozen=True)
class PlayerChange:
    endpoints: frozenset[str]
    old_club_ids: tuple[str, ...]
    new_club_ids: tuple[str, ...]


def _players(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Mapping[str, Any]]]:
    result: dict[str, dict[str, Mapping[str, Any]]] = {}
    for row in rows:
        player = _id('player_id', row.get('player_id'))
        club = _id('club_id', row.get('club_id'))
        existing = result.setdefault(player, {}).get(club)
        if existing is not None and dict(existing) != dict(row):
            raise CurrentCaptureError('conflicting player membership rows')
        result[player][club] = row
    return result


def player_changes(
    previous_full_rows: Iterable[Mapping[str, Any]], new_full_rows: Iterable[Mapping[str, Any]],
) -> dict[str, PlayerChange]:
    """Select careers by MV/date or membership; contract changes need no career."""
    old, new = _players(previous_full_rows), _players(new_full_rows)
    changes: dict[str, PlayerChange] = {}
    for player in sorted(old.keys() | new.keys()):
        endpoints: set[str] = set()
        old_clubs, new_clubs = tuple(sorted(old.get(player, {}))), tuple(sorted(new.get(player, {})))
        if player not in old:
            endpoints.update(('market_value_points', 'transfer_events'))
        elif old_clubs != new_clubs:
            endpoints.add('transfer_events')
        if player in old and player in new:
            def values(memberships: Mapping[str, Mapping[str, Any]]) -> set[str]:
                return {_json([row.get('market_value_eur'),
                               row.get('market_value_date', row.get('market_value_updated_at'))])
                        for row in memberships.values()}
            if values(old[player]) != values(new[player]):
                endpoints.add('market_value_points')
        if endpoints:
            changes[player] = PlayerChange(frozenset(endpoints), old_clubs, new_clubs)
    return changes


def _validated_rows(club_id: str, rows: Any, *, allow_empty: bool = False) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(rows, (list, tuple)) or (not rows and not allow_empty):
        raise CurrentCaptureError('club roster must contain full parsed player rows')
    output: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for value in rows:
        if not isinstance(value, Mapping) or not FULL_PLAYER_FIELDS <= value.keys():
            raise CurrentCaptureError('club roster lost full /plus/1 player fields')
        row = dict(value)
        player = _id('player_id', row['player_id'])
        if _id('club_id', row['club_id']) != club_id:
            raise CurrentCaptureError('player row belongs to another club')
        if player in seen:
            raise CurrentCaptureError('duplicate membership inside club roster')
        seen.add(player)
        row['player_id'], row['club_id'] = player, club_id
        for field in ('player_slug', 'name'):
            _text(field, row[field])
        for field in ('market_value_eur', 'age', 'height_cm'):
            number = row[field]
            if number is not None and (isinstance(number, bool) or not isinstance(number, (int, float))
                                       or not math.isfinite(number) or number < 0):
                raise CurrentCaptureError(f'{field} must be a finite nonnegative number or null')
        output.append(MappingProxyType(row))
    return tuple(output)


@dataclass(frozen=True)
class ClubRosterSnapshot:
    club_id: str
    rows: tuple[Mapping[str, Any], ...]
    raw_capture_id: str
    raw_fetched_at: datetime
    source_url: str
    source_body_hash: str
    bronze_manifest: str | None
    signature: str
    applicability_status: str = 'ok'
    authoritative_empty_proof: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, 'club_id', _id('club_id', self.club_id))
        if self.applicability_status not in {'ok', 'authoritative_empty'}:
            raise CurrentCaptureError('club applicability must be ok or authoritative_empty')
        object.__setattr__(self, 'rows', _validated_rows(
            self.club_id, self.rows, allow_empty=self.applicability_status == 'authoritative_empty',
        ))
        _text('raw_capture_id', self.raw_capture_id)
        object.__setattr__(self, 'raw_fetched_at', _timestamp(self.raw_fetched_at))
        for field in ('source_body_hash', 'signature'):
            _hash(field, getattr(self, field))
        url = urlsplit(self.source_url)
        if url.scheme != 'https' or not url.netloc or url.username or url.password:
            raise CurrentCaptureError('squad source_url must be credential-free HTTPS')
        if not re.search(r'/verein/' + re.escape(self.club_id) + r'(?:/|$)', url.path) or not re.search(r'/plus/1(?:/|$)', url.path):
            raise CurrentCaptureError('raw source_url does not prove full club /plus/1 capture')
        if self.bronze_manifest is not None:
            _text('bronze_manifest', self.bronze_manifest)
        if self.applicability_status == 'authoritative_empty':
            proof = self.authoritative_empty_proof
            expected = {'kind': 'typed_fetch_state', 'status': 'authoritative_empty',
                        'raw_capture_id': self.raw_capture_id, 'source_body_hash': self.source_body_hash}
            if self.rows or not isinstance(proof, Mapping) or dict(proof) != expected:
                raise CurrentCaptureError('authoritative empty requires matching typed raw proof and no rows')
            object.__setattr__(self, 'authoritative_empty_proof', MappingProxyType(dict(proof)))
        elif self.authoritative_empty_proof is not None:
            raise CurrentCaptureError('ok club must not contain authoritative empty proof')

    def as_dict(self) -> dict[str, Any]:
        return {'rows': [dict(row) for row in self.rows], 'raw_capture_id': self.raw_capture_id,
                'raw_fetched_at': self.raw_fetched_at.isoformat(), 'source_url': self.source_url,
                'source_body_hash': self.source_body_hash, 'bronze_manifest': self.bronze_manifest,
                'signature': self.signature, 'applicability_status': self.applicability_status,
                'authoritative_empty_proof': (dict(self.authoritative_empty_proof)
                                              if self.authoritative_empty_proof is not None else None)}

    @classmethod
    def from_mapping(
        cls, club_id: str, value: Mapping[str, Any], *, require_committed: bool = False,
    ) -> 'ClubRosterSnapshot':
        fields = {'rows', 'raw_capture_id', 'raw_fetched_at', 'source_url', 'source_body_hash',
                  'bronze_manifest', 'signature'}
        if not fields <= set(value) or set(value) - fields - {'applicability_status', 'authoritative_empty_proof'}:
            raise CurrentCaptureError('club snapshot fields differ')
        if require_committed and not value['bronze_manifest']:
            raise CurrentCaptureError('retained roster lacks committed Bronze manifest')
        return cls(club_id=club_id, **value)


@dataclass(frozen=True)
class FullRosterSnapshot:
    scope_id: str
    competition_id: str
    edition_id: str
    squad_saison_id: int
    expected_team_ids: tuple[str, ...]
    clubs: Mapping[str, ClubRosterSnapshot]
    changed_club_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _text('scope_id', self.scope_id)
        _text('competition_id', self.competition_id)
        _text('edition_id', self.edition_id)
        if self.scope_id != deterministic_scope_id(self.competition_id, self.edition_id):
            raise CurrentCaptureError('roster scope does not match registry competition/edition')
        if isinstance(self.squad_saison_id, bool) or not isinstance(self.squad_saison_id, int) or not 1800 <= self.squad_saison_id <= 2199:
            raise CurrentCaptureError('squad_saison_id must be an explicit source year')
        expected = tuple(sorted(_id('expected club', club) for club in self.expected_team_ids))
        if not expected or len(set(expected)) != len(expected):
            raise CurrentCaptureError('expected clubs must be nonempty and unique')
        if set(self.clubs) != set(expected):
            raise CurrentCaptureError('full roster must cover every expected club')
        clubs = dict(self.clubs)
        for club_id, club in clubs.items():
            if not isinstance(club, ClubRosterSnapshot) or club.club_id != club_id:
                raise CurrentCaptureError('club snapshot identity differs')
            if not re.search(r'/saison_id/' + str(self.squad_saison_id) + r'(?:/|$)', urlsplit(club.source_url).path):
                raise CurrentCaptureError('club raw capture belongs to another edition')
        changed = tuple(sorted(self.changed_club_ids))
        if not set(changed) <= set(expected):
            raise CurrentCaptureError('changed clubs differ from expected participants')
        object.__setattr__(self, 'expected_team_ids', expected)
        object.__setattr__(self, 'clubs', MappingProxyType(clubs))
        object.__setattr__(self, 'changed_club_ids', changed)

    @property
    def rows(self) -> list[dict[str, Any]]:
        rows = []
        for club_id in self.expected_team_ids:
            club = self.clubs[club_id]
            for row in club.rows:
                rows.append({**dict(row), '_source_url': club.source_url,
                             '_source_body_hash': club.source_body_hash,
                             '_source_fetched_at': club.raw_fetched_at.isoformat()})
        return rows

    def as_dict(self) -> dict[str, Any]:
        return {'scope_id': self.scope_id, 'competition_id': self.competition_id,
                'edition_id': self.edition_id, 'squad_saison_id': self.squad_saison_id,
                'expected_team_ids': list(self.expected_team_ids),
                'clubs': {club_id: club.as_dict() for club_id, club in self.clubs.items()}}

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> 'FullRosterSnapshot':
        if set(value) != {'scope_id', 'competition_id', 'edition_id', 'squad_saison_id', 'expected_team_ids', 'clubs'}:
            raise CurrentCaptureError('full roster snapshot fields differ')
        if not isinstance(value['clubs'], Mapping):
            raise CurrentCaptureError('club snapshots must be an object')
        clubs = {str(club_id): ClubRosterSnapshot.from_mapping(str(club_id), club, require_committed=True)
                 for club_id, club in value['clubs'].items()}
        return cls(scope_id=value['scope_id'], competition_id=value['competition_id'],
                   edition_id=value['edition_id'], squad_saison_id=value['squad_saison_id'],
                   expected_team_ids=value['expected_team_ids'], clubs=clubs)


def assemble_full_roster(
    snapshot: FullRosterSnapshot | None, expected_team_ids: Iterable[str],
    completed_updated_clubs: Mapping[str, ClubRosterSnapshot | Mapping[str, Any]],
    required_changed_ids: Iterable[str], *, scope_id: str | None = None,
    competition_id: str | None = None, edition_id: str | None = None,
    squad_saison_id: int | None = None,
    participant_membership_verified: bool = False,
) -> FullRosterSnapshot:
    """All changed and new clubs must complete; retain only verified old clubs."""
    expected = tuple(_id('expected club', club) for club in expected_team_ids)
    expected_set = set(expected)
    required = {_id('changed club', club) for club in required_changed_ids}
    if snapshot is not None and scope_id is not None and snapshot.scope_id != scope_id:
        raise CurrentCaptureError('retained roster belongs to another scope')
    if snapshot is not None:
        for name, requested in (('competition_id', competition_id), ('edition_id', edition_id),
                                ('squad_saison_id', squad_saison_id)):
            if requested is not None and requested != getattr(snapshot, name):
                raise CurrentCaptureError(f'retained roster {name} differs from registry')
        identity, competition, edition, saison = (snapshot.scope_id, snapshot.competition_id,
                                                   snapshot.edition_id, snapshot.squad_saison_id)
    else:
        identity = _text('scope_id', scope_id)
        competition = _text('competition_id', competition_id)
        edition = _text('edition_id', edition_id)
        saison = squad_saison_id
    if snapshot is not None and expected_set != set(snapshot.expected_team_ids) and not participant_membership_verified:
        raise CurrentCaptureError('participant membership change requires verified listing proof')
    if not required <= expected_set or not set(completed_updated_clubs) <= expected_set:
        raise CurrentCaptureError('updated clubs differ from current participants')
    if not required <= set(completed_updated_clubs):
        raise CurrentCaptureError('changed roster remains incomplete or pending')
    clubs: dict[str, ClubRosterSnapshot] = {}
    for club_id in expected:
        if club_id in completed_updated_clubs:
            value = completed_updated_clubs[club_id]
            club = value if isinstance(value, ClubRosterSnapshot) else ClubRosterSnapshot.from_mapping(club_id, value)
            if snapshot is not None and club_id in snapshot.clubs:
                old = snapshot.clubs[club_id]
                if club.raw_fetched_at < old.raw_fetched_at:
                    raise CurrentCaptureError('updated club capture is older than retained data')
                if club.applicability_status == 'authoritative_empty' and old.rows:
                    raise CurrentCaptureError('90% completeness guard refuses emptying populated club')
                for field in _BIO_FIELDS:
                    if any(row[field] is not None for row in old.rows) and not any(
                        row[field] is not None for row in club.rows
                    ):
                        raise CurrentCaptureError(f'updated full squad lost {field} evidence')
        elif snapshot is not None and club_id in snapshot.clubs:
            club = snapshot.clubs[club_id]
            if not club.bronze_manifest:
                raise CurrentCaptureError('retained roster lacks committed Bronze manifest')
        else:
            raise CurrentCaptureError('new participant has no completed full squad')
        clubs[club_id] = club
    if snapshot is not None and sum(len(club.rows) for club in clubs.values()) < 0.9 * sum(
        len(club.rows) for club in snapshot.clubs.values()
    ):
        raise CurrentCaptureError('assembled full roster is below existing 90% completeness guard')
    return FullRosterSnapshot(identity, competition, edition, saison, expected, clubs,
                              tuple(completed_updated_clubs))
