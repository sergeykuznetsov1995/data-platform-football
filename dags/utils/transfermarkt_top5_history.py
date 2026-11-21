"""Offline acceptance contract for Transfermarkt top-5 native history (#1403)."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from scrapers.transfermarkt.registry import deterministic_scope_id


DEFAULT_MATRIX_PATH = (
    Path(__file__).resolve().parents[1]
    / 'configs'
    / 'transfermarkt'
    / 'top5_history_acceptance.json'
)
LEGACY_PLAYERS_TABLE = 'iceberg.bronze.transfermarkt_players'
COMPETITIONS = (
    ('GB1', 'ENG-Premier League'),
    ('ES1', 'ESP-La Liga'),
    ('IT1', 'ITA-Serie A'),
    ('L1', 'GER-Bundesliga'),
    ('FR1', 'FRA-Ligue 1'),
)
EDITIONS = tuple(str(year) for year in range(2015, 2026))
ENTITY_TABLES = {
    'squad_memberships': 'iceberg.bronze.transfermarkt_squad_memberships',
    'player_attribute_observations': (
        'iceberg.bronze.transfermarkt_player_attribute_observations'
    ),
    'player_contract_observations': (
        'iceberg.bronze.transfermarkt_player_contract_observations'
    ),
    'market_value_points': 'iceberg.bronze.transfermarkt_market_value_points',
    'transfer_events': 'iceberg.bronze.transfermarkt_transfer_events',
}
ROSTER_ENTITIES = (
    'squad_memberships',
    'player_attribute_observations',
    'player_contract_observations',
)
RAW_LINEAGE = (
    'source_url',
    'source_body_hash',
    'fetched_at',
    '_ingested_at',
    'parser_revision',
    'schema_revision',
    'cycle_id',
    'scope_id',
    '_batch_id',
)


class Top5HistoryError(ValueError):
    """The offline acceptance matrix, pins, or results are not exact."""


@dataclass(frozen=True)
class Top5Scope:
    competition_id: str
    edition_id: str
    scope_id: str
    legacy_league: str
    legacy_season: str

    def as_dict(self) -> dict[str, str]:
        return {
            'competition_id': self.competition_id,
            'edition_id': self.edition_id,
            'scope_id': self.scope_id,
            'legacy_league': self.legacy_league,
            'legacy_season': self.legacy_season,
        }


def acceptance_scopes() -> tuple[Top5Scope, ...]:
    return tuple(
        Top5Scope(
            competition_id=competition_id,
            edition_id=edition_id,
            scope_id=deterministic_scope_id(competition_id, edition_id),
            legacy_league=legacy_league,
            legacy_season=f'{edition_id[2:]}{int(edition_id[2:]) + 1:02d}',
        )
        for competition_id, legacy_league in COMPETITIONS
        for edition_id in EDITIONS
    )


def scope_for(competition_id: str, edition_id: str) -> Top5Scope:
    key = (str(competition_id), str(edition_id))
    for scope in acceptance_scopes():
        if (scope.competition_id, scope.edition_id) == key:
            return scope
    raise Top5HistoryError(f'{key[0]}/{key[1]} is outside the #1403 target matrix')


def _matrix_entities() -> dict[str, dict[str, Any]]:
    return {
        entity: {
            'table': table,
            'scope_columns': (
                ['source_competition_id', 'source_edition_id']
                if entity in {'market_value_points', 'transfer_events'}
                else ['competition_id', 'edition_id']
            ),
        }
        for entity, table in ENTITY_TABLES.items()
    }


def load_acceptance_matrix(
    path: Path = DEFAULT_MATRIX_PATH,
) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        raise Top5HistoryError('acceptance matrix is unreadable') from exc
    if not isinstance(value, dict):
        raise Top5HistoryError('acceptance matrix must be a JSON object')
    if value.get('version') != 1 or type(value.get('version')) is not int:
        raise Top5HistoryError('acceptance matrix version must be integer 1')
    if value.get('issue') != 1403 or type(value.get('issue')) is not int:
        raise Top5HistoryError('acceptance matrix must belong to issue 1403')
    if value.get('entities') != _matrix_entities():
        raise Top5HistoryError('acceptance matrix entities do not match the contract')
    if value.get('scopes') != [item.as_dict() for item in acceptance_scopes()]:
        raise Top5HistoryError('acceptance matrix scopes do not match the contract')
    return value


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _validated_pins(
    snapshots: Mapping[str, int],
    expected_tables: Iterable[str],
) -> dict[str, int]:
    expected = set(expected_tables)
    if set(snapshots) != expected:
        missing = sorted(expected - set(snapshots))
        extra = sorted(set(snapshots) - expected)
        raise Top5HistoryError(
            f'snapshot pins do not match required tables; missing={missing}, extra={extra}'
        )
    result = {}
    for table, snapshot in snapshots.items():
        if type(snapshot) is not int or snapshot <= 0:
            raise Top5HistoryError(f'{table}: snapshot must be a positive integer')
        result[table] = snapshot
    return result


def _lineage_complete(alias='') -> str:
    prefix = f'{alias}.' if alias else ''
    return '(' + ' AND '.join(
        f'{prefix}{column} IS NOT NULL' for column in RAW_LINEAGE
    ) + ')'


def build_coverage_sql(snapshots: Mapping[str, int]) -> str:
    pins = _validated_pins(snapshots, ENTITY_TABLES.values())
    scope_values = ',\n    '.join(
        f'({_sql_literal(scope.competition_id)}, {_sql_literal(scope.edition_id)}, '
        f'{_sql_literal(scope.legacy_league)}, {_sql_literal(scope.legacy_season)})'
        for scope in acceptance_scopes()
    )
    entity_values = ', '.join(
        f'({_sql_literal(entity)})' for entity in ENTITY_TABLES
    )
    competition_ids = ', '.join(
        _sql_literal(competition_id) for competition_id, _ in COMPETITIONS
    )
    edition_ids = ', '.join(_sql_literal(edition) for edition in EDITIONS)
    legacy_leagues = ', '.join(
        _sql_literal(league) for _, league in COMPETITIONS
    )
    legacy_seasons = ', '.join(
        _sql_literal(f'{edition[2:]}{int(edition[2:]) + 1:02d}')
        for edition in EDITIONS
    )
    actual = []
    for entity, table in ENTITY_TABLES.items():
        source_scope = entity in {'market_value_points', 'transfer_events'}
        competition = 'source_competition_id' if source_scope else 'competition_id'
        edition = 'source_edition_id' if source_scope else 'edition_id'
        if source_scope or entity == 'player_contract_observations':
            legacy = (
                'CAST(NULL AS varchar) legacy_league, '
                'CAST(NULL AS varchar) legacy_season,'
            )
            where = (
                f'{competition} IN ({competition_ids}) AND '
                f'{edition} IN ({edition_ids})'
            )
        else:
            legacy = 'league legacy_league, season legacy_season,'
            where = (
                f'({competition} IN ({competition_ids}) AND {edition} IN ({edition_ids})) '
                f'OR ({competition} IS NULL AND {edition} IS NULL '
                f'AND league IN ({legacy_leagues}) AND season IN ({legacy_seasons}))'
            )
        actual.append(f"""SELECT {_sql_literal(entity)} entity,
       {competition} competition_id, {edition} edition_id,
       {legacy}
       ({competition} IS NULL AND {edition} IS NULL) null_scope,
       {_lineage_complete()} lineage_complete
FROM {table} FOR VERSION AS OF {pins[table]}
WHERE {where}""")
    return f"""WITH scopes(competition_id, edition_id, legacy_league, legacy_season) AS (
  VALUES
    {scope_values}
), entities(entity) AS (VALUES {entity_values}),
expected AS (
  SELECT e.entity, s.*
  FROM entities e CROSS JOIN scopes s
), actual AS (
  {' UNION ALL '.join(actual)}
)
SELECT e.entity, e.competition_id, e.edition_id,
       e.legacy_league, e.legacy_season,
       COUNT_IF(a.competition_id IS NOT NULL AND a.edition_id IS NOT NULL) native_rows,
       COUNT_IF(a.competition_id IS NOT NULL AND a.edition_id IS NOT NULL
                AND a.lineage_complete) full_lineage_rows,
       COUNT_IF(COALESCE(a.null_scope, FALSE)) null_scope_rows
FROM expected e
LEFT JOIN actual a ON a.entity = e.entity
 AND ((a.competition_id = e.competition_id AND a.edition_id = e.edition_id)
      OR (COALESCE(a.null_scope, FALSE)
          AND a.legacy_league = e.legacy_league
          AND a.legacy_season = e.legacy_season))
GROUP BY e.entity, e.competition_id, e.edition_id, e.legacy_league, e.legacy_season
ORDER BY e.entity, e.competition_id, e.edition_id"""


def _result_number(row: Mapping[str, Any], field: str) -> int:
    value = row.get(field)
    if type(value) is not int or value < 0:
        raise Top5HistoryError(f'{field} must be a nonnegative integer')
    return value


def coverage_violations(
    rows: Iterable[Mapping[str, Any]],
    *,
    scopes: Sequence[Top5Scope] | None = None,
) -> list[dict[str, Any]]:
    target_scopes = tuple(scopes if scopes is not None else acceptance_scopes())
    expected = {
        (entity, scope.competition_id, scope.edition_id): scope
        for scope in target_scopes
        for entity in ENTITY_TABLES
    }
    counts = Counter()
    by_identity = {}
    violations = []
    for row in rows:
        identity = (
            str(row.get('entity')),
            str(row.get('competition_id')),
            str(row.get('edition_id')),
        )
        counts[identity] += 1
        by_identity[identity] = row
        if identity not in expected:
            violations.append({
                'kind': 'unexpected_result',
                'entity': identity[0],
                'competition_id': identity[1],
                'edition_id': identity[2],
            })
    for identity, count in sorted(counts.items()):
        if count > 1:
            violations.append({
                'kind': 'duplicate_result',
                'entity': identity[0],
                'competition_id': identity[1],
                'edition_id': identity[2],
            })
    for identity, scope in expected.items():
        entity, competition_id, edition_id = identity
        base = {
            'entity': entity,
            'competition_id': competition_id,
            'edition_id': edition_id,
        }
        if counts[identity] == 0:
            violations.append({'kind': 'missing_result', **base})
            continue
        if counts[identity] > 1:
            continue
        row = by_identity[identity]
        try:
            native_rows = _result_number(row, 'native_rows')
            lineage_rows = _result_number(row, 'full_lineage_rows')
            null_rows = _result_number(row, 'null_scope_rows')
        except Top5HistoryError:
            violations.append({'kind': 'invalid_result', **base})
            continue
        if native_rows == 0:
            violations.append({'kind': 'missing_native', **base})
        if lineage_rows != native_rows:
            violations.append({
                'kind': 'incomplete_lineage',
                **base,
                'native_rows': native_rows,
                'full_lineage_rows': lineage_rows,
            })
        if null_rows:
            violations.append({
                'kind': 'null_scope',
                **base,
                'null_scope_rows': null_rows,
                'legacy_league': scope.legacy_league,
                'legacy_season': scope.legacy_season,
            })
    return violations


def build_roster_reconciliation_sql(
    scope: Top5Scope,
    snapshots: Mapping[str, int],
) -> str:
    if scope not in acceptance_scopes():
        raise Top5HistoryError('reconciliation scope is outside the #1403 matrix')
    required = [ENTITY_TABLES[entity] for entity in ROSTER_ENTITIES]
    required.append(LEGACY_PLAYERS_TABLE)
    pins = _validated_pins(snapshots, required)
    parts = []
    for entity in ROSTER_ENTITIES:
        table = ENTITY_TABLES[entity]
        club = 'team_id' if entity == 'player_contract_observations' else 'club_id'
        parts.append(f"""SELECT {_sql_literal(entity)} entity, COUNT(*) key_groups,
       COALESCE(SUM(n_rows), 0) native_rows,
       COALESCE(SUM(l_rows), 0) legacy_rows,
       COUNT_IF(n_rows IS NULL) legacy_only_groups,
       COUNT_IF(l_rows IS NULL) native_only_groups,
       COUNT_IF(n_rows IS DISTINCT FROM l_rows) count_mismatch_groups
FROM (
  SELECT n.rows n_rows, l.rows l_rows
  FROM (
    SELECT player_id, {club} club_id, COUNT(*) rows
    FROM {table} FOR VERSION AS OF {pins[table]}
    WHERE competition_id = {_sql_literal(scope.competition_id)}
      AND edition_id = {_sql_literal(scope.edition_id)}
    GROUP BY player_id, {club}
  ) n
  FULL OUTER JOIN (
    SELECT player_id, current_club_id club_id, COUNT(*) rows
    FROM {LEGACY_PLAYERS_TABLE} FOR VERSION AS OF {pins[LEGACY_PLAYERS_TABLE]}
    WHERE league = {_sql_literal(scope.legacy_league)}
      AND season = {_sql_literal(scope.legacy_season)}
    GROUP BY player_id, current_club_id
  ) l ON n.player_id = l.player_id AND n.club_id IS NOT DISTINCT FROM l.club_id
) compared""")
    return 'WITH reconciliation AS (\n' + '\nUNION ALL\n'.join(parts) + '\n)\n' + \
        'SELECT * FROM reconciliation ORDER BY entity'


def reconciliation_violations(
    rows: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    counts = Counter()
    by_entity = {}
    violations = []
    for row in rows:
        entity = str(row.get('entity'))
        counts[entity] += 1
        by_entity[entity] = row
        if entity not in ROSTER_ENTITIES:
            violations.append({'kind': 'unexpected_result', 'entity': entity})
    for entity, count in sorted(counts.items()):
        if count > 1:
            violations.append({'kind': 'duplicate_result', 'entity': entity})
    for entity in ROSTER_ENTITIES:
        if counts[entity] == 0:
            violations.append({'kind': 'missing_result', 'entity': entity})
            continue
        if counts[entity] > 1:
            continue
        row = by_entity[entity]
        try:
            native_rows = _result_number(row, 'native_rows')
            legacy_rows = _result_number(row, 'legacy_rows')
            native_only = _result_number(row, 'native_only_groups')
            legacy_only = _result_number(row, 'legacy_only_groups')
            mismatches = _result_number(row, 'count_mismatch_groups')
            _result_number(row, 'key_groups')
        except Top5HistoryError:
            violations.append({'kind': 'invalid_result', 'entity': entity})
            continue
        if native_rows == 0:
            violations.append({'kind': 'missing_native', 'entity': entity})
        if legacy_rows == 0:
            violations.append({'kind': 'missing_legacy', 'entity': entity})
        for kind, value in (
            ('native_only', native_only),
            ('legacy_only', legacy_only),
            ('count_mismatch', mismatches),
        ):
            if value:
                violations.append({'kind': kind, 'entity': entity, 'value': value})
    return violations


__all__ = [
    'DEFAULT_MATRIX_PATH',
    'EDITIONS',
    'ENTITY_TABLES',
    'LEGACY_PLAYERS_TABLE',
    'RAW_LINEAGE',
    'ROSTER_ENTITIES',
    'Top5HistoryError',
    'Top5Scope',
    'acceptance_scopes',
    'build_coverage_sql',
    'build_roster_reconciliation_sql',
    'coverage_violations',
    'load_acceptance_matrix',
    'reconciliation_violations',
    'scope_for',
]
