"""Structured current injury observations from saved Transfermarkt HTML.

The existing current lane remains the source detector and fetcher. This module
only normalizes supplied captures; it does not change scheduling or writers.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import json
import re
from typing import Any

from bs4 import BeautifulSoup

CURRENT_INJURY_PARSER_REVISION = 'current-injuries-v1'
CURRENT_INJURY_SCHEMA_REVISION = '1'
CURRENT_INJURY_STATUSES = frozenset({'observed_present', 'observed_empty', 'not_applicable'})
CURRENT_INJURY_COLUMNS = (
    'competition_id', 'edition_id', 'player_id', 'club_id', 'injury',
    'since', 'expected_return',
)
CURRENT_INJURY_EPISODE_KEY = (
    'competition_id', 'edition_id', 'player_id', 'injury',
)
CURRENT_INJURY_OBSERVATION_KEY = CURRENT_INJURY_EPISODE_KEY + ('observed_at',)
_PLAYER_HREF_RE = re.compile(r'^/[^/]+/profil/spieler/([0-9]+)(?:[/?#].*)?$')
_CLUB_HREF_RE = re.compile(r'^/[^/]+/startseite/verein/([0-9]+)(?:[/?#].*)?$')
_COMPETITION_RE = re.compile(r'[A-Za-z0-9_-]+')
_DIGITS_RE = re.compile(r'[0-9]+')
_DATE_FORMATS = ('%b %d, %Y', '%B %d, %Y', '%Y-%m-%d', '%d.%m.%Y', '%d/%m/%Y')
_MISSING_DATES = frozenset({'', '-', '?', 'unknown'})
_HEADERS = {
    'player': 'player',
    'club': 'club',
    'injury': 'injury',
    'reason': 'injury',
    'since': 'since',
    'expected return': 'expected_return',
    'return expected': 'expected_return',
}


class CurrentInjuryParseError(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class CurrentInjury:
    competition_id: str
    edition_id: str
    player_id: str
    club_id: str
    injury: str
    since: str | None
    expected_return: str | None

    @property
    def episode_key(self) -> tuple[str, ...]:
        return self.competition_id, self.edition_id, self.player_id, self.injury

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CurrentInjuryResult:
    competition_id: str
    edition_id: str
    status: str
    episodes: tuple[CurrentInjury, ...]
    reconciled_duplicates: int = 0
    completeness_proof: str | None = None
    applicability_proof: str | None = None

    def __post_init__(self):
        if self.status not in CURRENT_INJURY_STATUSES:
            raise CurrentInjuryParseError('status_invalid')
        if (self.status != 'observed_present') == bool(self.episodes):
            raise CurrentInjuryParseError('status_invalid')
        if self.status == 'observed_empty':
            if not self.completeness_proof:
                raise CurrentInjuryParseError('completeness_proof_missing')
        elif self.completeness_proof is not None:
            raise CurrentInjuryParseError('completeness_proof_unexpected')
        if self.status == 'not_applicable':
            if not self.applicability_proof:
                raise CurrentInjuryParseError('applicability_proof_missing')
        elif self.applicability_proof is not None:
            raise CurrentInjuryParseError('applicability_proof_unexpected')

    def canonical_json(self) -> str:
        return json.dumps({
            'competition_id': self.competition_id,
            'edition_id': self.edition_id,
            'status': self.status,
            'reconciled_duplicates': self.reconciled_duplicates,
            'completeness_proof': self.completeness_proof,
            'applicability_proof': self.applicability_proof,
            'episodes': [episode.as_dict() for episode in self.episodes],
        }, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def parse_current_injuries(html: Any, *, competition_id: Any, edition_id: Any) -> CurrentInjuryResult:
    competition = _competition_id(competition_id)
    edition = _edition_id(edition_id)
    if not isinstance(html, str) or not html.strip():
        raise CurrentInjuryParseError('invalid_html')
    soup = BeautifulSoup(html, 'html.parser')
    tables = soup.select('table.items')
    if len(tables) != 1:
        raise CurrentInjuryParseError('injury_table_missing' if not tables else 'injury_table_ambiguous')
    table = tables[0]
    thead = table.find('thead', recursive=False)
    tbody = table.find('tbody', recursive=False)
    if thead is None or tbody is None:
        raise CurrentInjuryParseError('injury_table_structure_changed')
    headers = [' '.join(th.get_text(' ', strip=True).lower().split()) for th in thead.find_all('th')]
    fields = [_HEADERS.get(header) for header in headers]
    required = {'player', 'club', 'injury', 'since', 'expected_return'}
    if (not headers or None in fields or required - set(fields)
            or len(set(fields)) != len(fields)):
        raise CurrentInjuryParseError('injury_headers_changed')
    source_rows = tbody.find_all('tr', recursive=False)
    if not source_rows:
        raise CurrentInjuryParseError('empty_layout_unproven')
    episodes: list[CurrentInjury] = []
    seen: dict[tuple[str, ...], CurrentInjury] = {}
    reconciled = 0
    for tr in source_rows:
        episode = _row(tr, fields, competition, edition)
        previous = seen.get(episode.episode_key)
        if previous is None:
            seen[episode.episode_key] = episode
            episodes.append(episode)
        elif previous == episode:
            reconciled += 1
        else:
            raise CurrentInjuryParseError('injury_conflict')
    return CurrentInjuryResult(competition, edition, 'observed_present', tuple(episodes), reconciled)


def confirmed_empty_current_injuries(
    *, competition_id: Any, edition_id: Any, completeness_proof: str,
) -> CurrentInjuryResult:
    """Represent empty only after an external full-layout proof, never from a missing table."""
    if not isinstance(completeness_proof, str) or not completeness_proof:
        raise CurrentInjuryParseError('completeness_proof_missing')
    return CurrentInjuryResult(
        _competition_id(competition_id), _edition_id(edition_id), 'observed_empty', (),
        completeness_proof=completeness_proof,
    )


def not_applicable_current_injuries(
    *, competition_id: Any, edition_id: Any, applicability_proof: str,
) -> CurrentInjuryResult:
    if not isinstance(applicability_proof, str) or not applicability_proof:
        raise CurrentInjuryParseError('applicability_proof_missing')
    return CurrentInjuryResult(
        _competition_id(competition_id), _edition_id(edition_id), 'not_applicable', (),
        applicability_proof=applicability_proof,
    )


def _row(tr, fields, competition: str, edition: str) -> CurrentInjury:
    cells = tr.find_all('td', recursive=False)
    if len(cells) != len(fields):
        raise CurrentInjuryParseError('injury_row_shape_changed')
    values = {field: cells[index] for index, field in enumerate(fields)}
    player = _linked_id(values['player'], _PLAYER_HREF_RE, 'player_identity_invalid')
    club = _linked_id(values['club'], _CLUB_HREF_RE, 'club_identity_invalid')
    injury = values['injury'].get_text(' ', strip=True)
    if not injury or injury in {'-', '?'}:
        raise CurrentInjuryParseError('injury_reason_invalid')
    since = _optional_date(values.get('since'), 'since_invalid')
    expected_return = _optional_date(values.get('expected_return'), 'expected_return_invalid')
    if since is not None and expected_return is not None and expected_return < since:
        raise CurrentInjuryParseError('date_order_invalid')
    return CurrentInjury(competition, edition, player, club, injury, since, expected_return)


def _linked_id(cell, pattern, reason: str) -> str:
    ids = set()
    for anchor in cell.find_all('a', href=True):
        href = anchor['href']
        if '/profil/spieler/' not in href and '/startseite/verein/' not in href:
            continue
        match = pattern.match(href)
        if match is None:
            raise CurrentInjuryParseError(reason)
        ids.add(str(int(match.group(1))))
    if len(ids) != 1:
        raise CurrentInjuryParseError(reason)
    return ids.pop()


def _optional_date(cell, reason: str) -> str | None:
    if cell is None:
        return None
    raw = cell.get_text(' ', strip=True)
    if raw.lower() in _MISSING_DATES or raw.lower().startswith('return unknown'):
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    raise CurrentInjuryParseError(reason)


def _competition_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise CurrentInjuryParseError('competition_id_invalid')
    rendered = str(value)
    if not _COMPETITION_RE.fullmatch(rendered):
        raise CurrentInjuryParseError('competition_id_invalid')
    return rendered


def _edition_id(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise CurrentInjuryParseError('edition_id_invalid')
    rendered = str(value)
    if not _DIGITS_RE.fullmatch(rendered) or not 1800 <= int(rendered) <= 2199:
        raise CurrentInjuryParseError('edition_id_invalid')
    return str(int(rendered))


__all__ = [
    'CURRENT_INJURY_COLUMNS', 'CURRENT_INJURY_EPISODE_KEY',
    'CURRENT_INJURY_OBSERVATION_KEY', 'CURRENT_INJURY_PARSER_REVISION',
    'CURRENT_INJURY_SCHEMA_REVISION', 'CURRENT_INJURY_STATUSES',
    'CurrentInjury', 'CurrentInjuryParseError', 'CurrentInjuryResult',
    'confirmed_empty_current_injuries', 'not_applicable_current_injuries',
    'parse_current_injuries',
]
