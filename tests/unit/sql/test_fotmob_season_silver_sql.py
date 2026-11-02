"""FotMob Silver season family (#2018): the 8 ``dags/sql/silver/fotmob_*.sql.j2`` files for
competitions, seasons, winner history, stages, standings, season teams, leaderboards, playoffs.

The SQL is Trino SQL (production). Here it is rendered with ``render_sql_template``, transpiled by
sqlglot to DuckDB (as in ``test_fotmob_match_silver_sql.py``) and executed over bronze ``_current``
rows built by the real FotMob parsers from saved pages (``tests/fixtures/fotmob_silver/seasons``,
``tests/fixtures/fotmob/league_8974_2022_null_playoff.json``) and assembled as
``FotMobIngestService`` does, + repository JSON encoding. Bronze column types come from the bronze
schema snapshot (``tests/fixtures/bronze_schemas.json``).

Expected rows come from an independent Python oracle that reads every column through its ``from``
path in ``configs/medallion/fotmob_silver/core_schema.yaml``; only ``derived:`` columns and the lead
decisions of #2018 (dedup tie-breaks, team-leaderboard ``participant_id`` 0, category dedup) are
spelled out here.
"""
from __future__ import annotations

import copy
import functools
import gzip
import json
import math
import os
import re
import sys
from dataclasses import asdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

sqlglot = pytest.importorskip('sqlglot')
duckdb = pytest.importorskip('duckdb')

ROOT = Path(__file__).resolve().parents[3]
SILVER = ROOT / 'dags' / 'sql' / 'silver'
FIXTURES = ROOT / 'tests' / 'fixtures'
SEASONS_DIR = FIXTURES / 'fotmob_silver' / 'seasons'
SCHEMA_PATH = ROOT / 'configs' / 'medallion' / 'fotmob_silver' / 'core_schema.yaml'

_DAGS_DIR = ROOT / 'dags'
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))
os.environ.setdefault('MEDALLION_CONFIG_DIR', str(ROOT / 'configs' / 'medallion'))

from scrapers.fotmob.catalog import classify_competition, discover_competitions, parse_seasons  # noqa: E402
from scrapers.fotmob.domain import ScopeRef  # noqa: E402
from scrapers.fotmob.parsers import parse_leaderboards, parse_season_bundle  # noqa: E402
from scrapers.fotmob.repository import normalize_rows  # noqa: E402

pytestmark = pytest.mark.unit

TABLES = (
    'fotmob_competitions', 'fotmob_competition_seasons', 'fotmob_competition_season_history',
    'fotmob_season_stages', 'fotmob_standings', 'fotmob_season_teams', 'fotmob_leaderboards',
    'fotmob_playoff_brackets',
)
CONTEXT_TABLES = TABLES[3:]
VIEWS = (
    'fotmob_competitions_current', 'fotmob_competition_seasons_current',
    'fotmob_competition_season_history_current', 'fotmob_season_stages_current',
    'fotmob_standings_current', 'fotmob_season_teams_current', 'fotmob_leaderboards_current',
    'fotmob_leaderboard_categories_current', 'fotmob_playoff_brackets_current',
)
BRONZE_SOURCES = {f'iceberg.bronze.{v}' for v in VIEWS}
PLACEHOLDER = 'fotmob_season_scope'

SNAPSHOT = json.loads((FIXTURES / 'bronze_schemas.json').read_text(encoding='utf-8'))['tables']
BRONZE_TYPES = {'varchar': 'VARCHAR', 'bigint': 'BIGINT', 'double': 'DOUBLE', 'boolean': 'BOOLEAN',
                'timestamp(6)': 'TIMESTAMP'}
DUCKDB_TYPES = {
    'bigint': 'BIGINT', 'integer': 'INTEGER', 'double': 'DOUBLE', 'boolean': 'BOOLEAN',
    'varchar': 'VARCHAR', 'date': 'DATE', 'timestamp(6)': 'TIMESTAMP',
    'array(varchar)': 'VARCHAR[]', 'array(bigint)': 'BIGINT[]',
}
T0 = datetime(2026, 10, 1, 3, 0, 0)
KEY_335 = '2026/2027 - Apertura'
KEY_47 = '2025/2026'

# Dedup order of each table (yaml dedup_order + lead decision 3 tie-breaks); NULLS LAST everywhere.
BASE_ORDER = (('_observed_at', 'desc'), ('_ingested_at', 'desc'))
TIES = {
    'fotmob_standings': (('position', 'asc'), ('table_name', 'asc')),
    'fotmob_leaderboards': (('rank', 'asc'), ('top_list_index', 'asc'), ('source_item_index', 'asc')),
    'fotmob_playoff_brackets': (('_target_batch_id', 'desc'), ('match_ids', 'asc')),
}
CATEGORY_ORDER = BASE_ORDER + (('source_order', 'asc'),)
SPLIT = re.compile(r'^(.+?) - (.+)$')


# ---------------------------------------------------------------------------- helpers

def _load_audit():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'audit_silver_charter_2018', ROOT / 'scripts' / 'audit_silver_charter.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


AUDIT = _load_audit()
SCHEMA = yaml.safe_load(SCHEMA_PATH.read_text(encoding='utf-8'))


def _sql_path(table: str) -> Path:
    return SILVER / f'{table}.sql.j2'


def _duckdb_sql(table: str, scope: str = 'TRUE') -> str:
    from utils.medallion_config import render_sql_template
    rendered = render_sql_template(_sql_path(table), fotmob_season_scope=scope)
    sql = sqlglot.transpile(rendered, read='trino', write='duckdb')[0]
    return sql.replace('iceberg.bronze.', 'bronze.')


def _load_gz(name: str):
    with gzip.open(SEASONS_DIR / name, 'rt', encoding='utf-8') as fh:
        return json.load(fh)


PAGE_335 = _load_gz('league_335_2026_2027_apertura.json.gz')
TOP_GOALS = _load_gz('leaderboard_47_goals.json.gz')
TOP_ASSISTS = _load_gz('leaderboard_47_goal_assist.json.gz')
PAGE_8974 = json.loads((FIXTURES / 'fotmob' / 'league_8974_2022_null_playoff.json').read_text(encoding='utf-8'))

# Synthetic allLeagues payload (the catalog is not in the fixtures): 47 is listed twice with a
# conflicting country name -> has_catalog_conflict; 9999 is a tombstone row (service.py).
CATALOG = {
    'popular': [{'id': 47, 'name': 'Premier League', 'ccode': 'ENG', 'countryName': 'England',
                 'pageUrl': '/leagues/47/overview/premier-league'}],
    'countries': [
        {'ccode': 'ENG', 'name': 'England (UK)', 'leagues': [
            {'id': 47, 'name': 'Premier League', 'pageUrl': '/leagues/47/overview/premier-league'}]},
        {'ccode': 'ARG', 'name': 'Argentina', 'leagues': [
            {'id': 112, 'name': 'Liga Profesional', 'pageUrl': '/leagues/112/overview/liga-profesional'}]},
        {'ccode': 'URU', 'name': 'Uruguay', 'leagues': [
            {'id': 230, 'name': 'Primera Division', 'pageUrl': '/leagues/230/overview/primera-division'}]},
        {'ccode': 'SLV', 'name': 'El Salvador', 'leagues': [
            {'id': 335, 'name': 'Primera Division', 'pageUrl': '/leagues/335/overview/primera-division'}]},
    ],
}
TOMBSTONE_ID = 9999
TEAM_TOP = {'TopLists': [{'StatName': 'rating_team', 'Title': 'FotMob rating', 'StatList': [
    {'ParticipantName': 'Arsenal', 'TeamId': 9825, 'StatValue': 7.31, 'Rank': 1, 'MatchesPlayed': 9},
    {'ParticipantName': 'Liverpool', 'TeamId': 8650, 'StatValue': 7.12, 'Rank': 2, 'MatchesPlayed': 9},
]}]}


# ---------------------------------------------------------------------------- bronze rows

def _lineage(observed: datetime, batch: str, entity: str) -> dict:
    return {'_target_batch_id': batch, '_payload_sha256': None, '_parser_version': 'fotmob-native-v2',
            '_raw_uri': None, '_source': 'fotmob', '_entity_type': entity,
            '_observed_at': observed, '_ingested_at': observed + timedelta(minutes=5)}


def _rows(rows, entity: str, *, observed: datetime = T0, batch: str | None = None) -> list[dict]:
    out = []
    for row in normalize_rows(rows):
        row.update(_lineage(observed, batch or f'{entity}-{observed:%H%M}', entity))
        out.append(row)
    return out


def competition_rows(catalog: dict, *, observed: datetime = T0) -> list[dict]:
    """Rows as ``FotMobIngestService`` writes fotmob_competitions (service.py:1432-1471)."""
    discovery = discover_competitions(catalog)
    conflicts = {c.competition_id: c for c in discovery.conflicts}
    rows = []
    for competition in discovery.competitions:
        conflict = conflicts.get(competition.competition_id)
        classification = classify_competition(competition, competition)
        rows.append({
            **asdict(competition),
            'competition_id': str(competition.competition_id),
            'presentation_slug': competition.presentation_slug,
            'scope_decision': classification.decision.value,
            'scope_reason': classification.reason,
            'scope_policy_rule': classification.policy_rule,
            'has_catalog_conflict': conflict is not None,
            'is_tombstoned': False,
            'tombstone_reason': None,
            'conflict_fields_json': list(conflict.fields if conflict else ()),
            'discovery_run_id': 'run-1',
            'discovery_date': observed.date().isoformat(),
        })
    rows.append({
        'competition_id': str(TOMBSTONE_ID), 'name': None, 'presentation_slug': str(TOMBSTONE_ID),
        'scope_decision': 'excluded', 'scope_reason': 'absent from two consecutive complete catalogs',
        'scope_policy_rule': 'two_snapshot_tombstone', 'has_catalog_conflict': False,
        'is_tombstoned': True, 'tombstone_reason': 'two_consecutive_complete_absences',
        'conflict_fields_json': [], 'discovery_run_id': 'run-1',
        'discovery_date': observed.date().isoformat(),
    })
    return _rows(rows, 'competitions', observed=observed)


def season_rows(payload: dict, competition_id: int, *, observed: datetime = T0) -> tuple[list, list]:
    """(competition_seasons, competition_season_history) rows as in service.py:1699-1790."""
    seasons = parse_seasons(payload, competition_id)
    items = [i for i in (payload.get('seasons') or []) if isinstance(i, dict) and i.get('seasonName') is not None]
    history_by_key = {str(i['seasonName']): i for i in items}
    links = {str(i['Name']): i for i in ((payload.get('stats') or {}).get('seasonStatLinks') or [])
             if isinstance(i, dict) and i.get('Name') is not None}
    history = [{
        'competition_id': str(competition_id), 'history_season_label': str(i['seasonName']),
        'winner_id': (i.get('winner') or {}).get('id'), 'winner_name': (i.get('winner') or {}).get('name'),
        'runner_up_id': (i.get('loser') or {}).get('id'), 'runner_up_name': (i.get('loser') or {}).get('name'),
        'season_history_json': i, 'discovered_at': observed, 'discovery_run_id': 'run-1',
    } for i in items]
    rows = []
    for season in seasons:
        item = history_by_key.get(season.source_season_key, {})
        rows.append({
            **asdict(season), 'competition_id': str(season.competition_id),
            'source_slug': payload.get('seostr'),
            'winner_id': (item.get('winner') or {}).get('id'),
            'winner_name': (item.get('winner') or {}).get('name'),
            'runner_up_id': (item.get('loser') or {}).get('id'),
            'runner_up_name': (item.get('loser') or {}).get('name'),
            'season_history_json': history_by_key.get(season.source_season_key),
            'stat_link_json': links.get(season.source_season_key),
            'discovered_at': observed, 'discovery_run_id': 'run-1',
        })
    return (_rows(rows, 'competition_seasons', observed=observed),
            _rows(history, 'competition_season_history', observed=observed))


def bundle_rows(payload: dict, competition_id: int, key: str, *, observed: datetime = T0) -> dict:
    """Season-page rows per view: parse_season_bundle + category assembly of service.py:2114-2123."""
    bundle = parse_season_bundle(payload, scope=ScopeRef(competition_id, key))
    categories = [{'competition_id': str(competition_id), 'source_season_key': key, **asdict(d)}
                  for d in (*bundle.player_categories, *bundle.team_categories)]
    return {
        'fotmob_season_stages_current': _rows(bundle.stages, 'season_stages', observed=observed),
        'fotmob_standings_current': _rows(bundle.standings, 'standings', observed=observed),
        'fotmob_season_teams_current': _rows(bundle.teams, 'season_teams', observed=observed),
        'fotmob_playoff_brackets_current': _rows(bundle.playoffs, 'playoff_brackets', observed=observed),
        'fotmob_leaderboard_categories_current': _rows(categories, 'leaderboard_categories', observed=observed),
    }


def leaderboard_rows(payload: dict, participant_type: str, competition_id: int, key: str,
                     *, observed: datetime = T0) -> list[dict]:
    rows = parse_leaderboards(payload, participant_type=participant_type,
                              scope=ScopeRef(competition_id, key))
    return _rows(rows, 'leaderboards', observed=observed)


def rescoped(payload: dict, competition_id: int, key: str) -> dict:
    """The same page served for another competition/season (details are what the parser checks)."""
    page = copy.deepcopy(payload)
    page['details'].update({'id': competition_id, 'selectedSeason': key, 'latestSeason': key})
    return page


def _to_bronze(value, dtype: str):
    if value is None:
        return None
    if dtype == 'varchar':
        if isinstance(value, bool):
            return 'true' if value else 'false'
        return value if isinstance(value, str) else str(value)
    if dtype == 'bigint':
        return int(value)
    if dtype == 'double':
        return float(value)
    if dtype == 'boolean':
        return bool(value)
    return value


class Bronze:
    """DuckDB with the 9 ``bronze.fotmob_*_current`` views typed as in the bronze snapshot."""

    def __init__(self, **rows):
        unknown = set(rows) - set(VIEWS)
        assert not unknown, unknown
        self.views = {v: [] for v in VIEWS}
        for view, view_rows in rows.items():
            types = SNAPSHOT[f'bronze.{view}']['columns']
            for r in view_rows:
                extra = set(r) - set(types)
                assert not extra, f'{view}: parser columns absent from bronze snapshot: {extra}'
                self.views[view].append({c: _to_bronze(r.get(c), t) for c, t in types.items()})
        self._con = None
        self._cache: dict = {}

    def con(self):
        if self._con is None:
            con = duckdb.connect(':memory:')
            con.execute("SET TimeZone = 'UTC'")
            con.execute('CREATE SCHEMA bronze')
            for view in VIEWS:
                cols = SNAPSHOT[f'bronze.{view}']['columns']
                ddl = ', '.join(f'"{c}" {BRONZE_TYPES[t]}' for c, t in cols.items())
                con.execute(f'CREATE TABLE bronze.{view} ({ddl})')
                if self.views[view]:
                    marks = ', '.join('?' for _ in cols)
                    con.executemany(f'INSERT INTO bronze.{view} VALUES ({marks})',
                                    [[r[c] for c in cols] for r in self.views[view]])
            self._con = con
        return self._con

    def run(self, table: str, scope: str = 'TRUE'):
        if (table, scope) not in self._cache:
            cur = self.con().execute(_duckdb_sql(table, scope))
            names = [d[0] for d in cur.description]
            types = [(d[0], str(d[1])) for d in cur.description]
            self._cache[(table, scope)] = ([dict(zip(names, r)) for r in cur.fetchall()], types)
        return self._cache[(table, scope)]

    def rows(self, table: str, scope: str = 'TRUE') -> list[dict]:
        return self.run(table, scope)[0]


# ---------------------------------------------------------------------------- oracle

_PATH_TOKEN = re.compile(r'\.([A-Za-z_][A-Za-z0-9_]*)|\["([^"]+)"\]|\[(\d+)\]')


def jpath(obj, path: str):
    """Evaluate a simple JSONPath ('$', '.name', '["name"]', '[N]'); missing -> None."""
    assert path.startswith('$'), path
    rest, pos = path[1:], 0
    while pos < len(rest):
        m = _PATH_TOKEN.match(rest, pos)
        assert m, f'unsupported path {path}'
        pos = m.end()
        name = m.group(1) or m.group(2)
        if name is not None:
            obj = obj.get(name) if isinstance(obj, dict) else None
        else:
            idx = int(m.group(3))
            obj = obj[idx] if isinstance(obj, list) and idx < len(obj) else None
    return obj


def _int(v):
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v.is_integer() else None
    if isinstance(v, str) and re.fullmatch(r'\s*-?\d+\s*', v):
        return int(v)
    return None


def _float(v):
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str) and v.strip().lower() in ('true', 'false'):
        return v.strip().lower() == 'true'
    return None


def _text(v):
    """Text of a JSON scalar as json_extract_scalar returns it."""
    if v is None or isinstance(v, (dict, list)):
        return None
    if isinstance(v, bool):
        return 'true' if v else 'false'
    return str(v)


def _date(v):
    if isinstance(v, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}', v):
        try:
            return date.fromisoformat(v)
        except ValueError:
            return None
    return None


def _id_array(v):
    try:
        parsed = json.loads(v) if isinstance(v, str) else None
    except ValueError:
        return None
    if not isinstance(parsed, list) or not all(isinstance(x, int) and not isinstance(x, bool) for x in parsed):
        return None
    return parsed


def convert(value, dtype: str):
    dtype = dtype.lower()
    if dtype in ('bigint', 'integer'):
        return _int(value)
    if dtype == 'double':
        return _float(value)
    if dtype == 'boolean':
        return _bool(value)
    if dtype == 'date':
        return _date(value)
    if dtype == 'array(bigint)':
        return _id_array(value)
    if dtype == 'array(varchar)':
        return None if not isinstance(value, list) else [_text(x) for x in value]
    if dtype == 'varchar':
        return _text(value)
    raise AssertionError(dtype)


def _loads(raw):
    return None if raw is None else json.loads(raw)


def resolve(source: str, ctx: dict):
    """Value of a yaml ``from`` (non-derived) in the row context ``ctx`` (view -> bronze row)."""
    view, _, rest = source.partition('.')
    row = ctx.get(view) or {}
    if ':' in rest:
        column, _, path = rest.partition(':')
        return jpath(_loads(row.get(column)), path)
    return row.get(rest)


def _cmp(order):
    def compare(a, b):
        for column, direction in order:
            x, y = a.get(column), b.get(column)
            if x == y:
                continue
            if x is None:
                return 1
            if y is None:
                return -1
            lt = x < y
            return (-1 if lt else 1) if direction == 'asc' else (1 if lt else -1)
        return 0
    return functools.cmp_to_key(compare)


def _valid(v):
    return v is not None and v != ''


def _pick(rows, key_cols, order):
    groups: dict = {}
    for r in rows:
        k = tuple(r[c] for c in key_cols)
        if all(_valid(x) for x in k):
            groups.setdefault(k, []).append(r)
    return [sorted(g, key=_cmp(order))[0] for g in groups.values()]


def _spec(table):
    return SCHEMA['tables'][table]


def _view_rows(bronze: Bronze, view: str) -> list[dict]:
    rows = []
    for r in bronze.views[view]:
        r = dict(r)
        r['competition_id'] = _int(r['competition_id'])
        rows.append(r)
    return rows


def _row(table: str, ctx: dict, derived: dict) -> dict:
    """One expected row: yaml ``from`` for non-derived columns, ``derived`` for the rest; conform
    ``try`` columns that fail -> NULL + issue (README: правило NULL)."""
    spec = _spec(table)
    main = ctx[spec['sources'][0]]
    out, issues = {}, []
    for col in spec['columns']:
        name = col['name']
        if name in derived:
            out[name] = derived[name]
            continue
        assert not str(col['from']).startswith('derived'), f'{table}.{name} needs a derived rule'
        raw = resolve(col['from'], ctx)
        value = convert(raw, col['type'])
        if col.get('conform') == 'try' and value is None and _valid(raw):
            issues.append({'column': name, 'raw': raw if isinstance(raw, str) else str(raw)})
        out[name] = value
    for col in spec.get('raw_json', []):
        out[col['name']] = _loads(resolve(col['from'], ctx))
    out.update({
        '_bronze_batch_id': main['_target_batch_id'], '_bronze_observed_at': main['_observed_at'],
        '_bronze_ingested_at': main['_ingested_at'], '_parser_version': main['_parser_version'],
        '_conform_issues_json': issues or None,
    })
    return out


def oracle(table: str, bronze: Bronze) -> list[dict]:
    spec = _spec(table)
    view = spec['sources'][0]
    key = spec['natural_key']
    rows = _view_rows(bronze, view)
    if table == 'fotmob_leaderboards':
        for r in rows:  # lead decision 4: team boards carry participant_id 0; a player needs an id
            if r['participant_type'] == 'team' and r['participant_id'] is None:
                r['participant_id'] = 0
            elif r['participant_id'] == 0:
                r['participant_id'] = None
    if table == 'fotmob_playoff_brackets':
        for r in rows:
            r['matchup_key'] = ':'.join([
                '-' if r['draw_order'] is None else str(r['draw_order']),
                'tbd' if r['home_team_id'] is None else str(r['home_team_id']),
                'tbd' if r['away_team_id'] is None else str(r['away_team_id'])])
    picked = _pick(rows, key, BASE_ORDER + TIES.get(table, ()))
    out = []
    if table == 'fotmob_competition_seasons':
        keys = {(r['competition_id'], r['source_season_key']) for r in picked}
        for r in picked:
            m = SPLIT.match(r['source_season_key'])
            parent = m.group(1) if m and (r['competition_id'], m.group(1)) in keys else None
            has_child = any(c == r['competition_id'] and (s := SPLIT.match(k)) and s.group(1) == r['source_season_key']
                            for c, k in keys)
            out.append(_row(table, {view: r}, {
                'parent_season_key': parent, 'season_part': m.group(2) if parent else None,
                'has_child_seasons': has_child}))
    elif table == 'fotmob_season_teams':
        stage_ids: dict = {}
        for r in rows:
            k = tuple(r[c] for c in key)
            if all(_valid(x) for x in k) and r['scope_stage_id'] is not None:
                stage_ids.setdefault(k, set()).add(r['scope_stage_id'])
        for r in picked:
            ids = stage_ids.get(tuple(r[c] for c in key))
            out.append(_row(table, {view: r}, {'scope_stage_ids': sorted(ids) if ids else None}))
    elif table == 'fotmob_leaderboards':
        cats = {}
        for c in _pick(_view_rows(bronze, 'fotmob_leaderboard_categories_current'),
                       ('competition_id', 'source_season_key', 'participant_type', 'name'), CATEGORY_ORDER):
            cats[(c['competition_id'], c['source_season_key'], c['participant_type'], c['name'])] = c
        for r in picked:
            cat = cats.get((r['competition_id'], r['source_season_key'], r['participant_type'], r['stat_name']))
            out.append(_row(table, {view: r, 'fotmob_leaderboard_categories_current': cat},
                            {'participant_id': r['participant_id']}))
    elif table == 'fotmob_playoff_brackets':
        out = [_row(table, {view: r}, {'matchup_key': r['matchup_key']}) for r in picked]
    else:
        out = [_row(table, {view: r}, {}) for r in picked]
    return out


# ---------------------------------------------------------------------------- comparison

def _close(a, b):
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b or a == b
    if isinstance(a, (int, float, Decimal)) and isinstance(b, (int, float, Decimal)):
        return math.isclose(float(a), float(b), rel_tol=1e-12, abs_tol=1e-12)
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_close(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_close(x, y) for x, y in zip(a, b))
    return a == b


def _same(column: str, dtype: str, got, want) -> bool:
    if column == '_conform_issues_json':
        if got is not None:
            issues = json.loads(got)
            if not issues or any(set(i) != {'column', 'raw', 'reason'} or not i['reason'] for i in issues):
                return False
            got = sorted(({'column': i['column'], 'raw': i['raw']} for i in issues), key=lambda i: i['column'])
        return got == (None if want is None else sorted(want, key=lambda i: i['column']))
    if column.endswith('_json'):
        return _close(None if got is None else json.loads(got), want)
    return _close(got, want)


def _key(table, row):
    return tuple(row[c] for c in _spec(table)['natural_key'])


def assert_matches_oracle(bronze: Bronze, table: str) -> list[dict]:
    got = bronze.rows(table)
    want = oracle(table, bronze)
    got_by = {_key(table, r): r for r in got}
    want_by = {_key(table, r): r for r in want}
    assert len(got_by) == len(got), f'{table}: duplicate natural key in SQL output'
    assert len(want_by) == len(want), f'{table}: duplicate natural key in oracle'
    assert set(got_by) == set(want_by), (
        f'{table}: keys differ; only SQL {sorted(set(got_by) - set(want_by), key=str)[:5]}, '
        f'only oracle {sorted(set(want_by) - set(got_by), key=str)[:5]}')
    expected = AUDIT.core_expected_columns(SCHEMA, table)
    mismatches = []
    for k, w in want_by.items():
        g = got_by[k]
        for column, dtype in expected.items():
            if not _same(column, dtype, g[column], w[column]):
                mismatches.append((k, column, g[column], w[column]))
    assert not mismatches, f'{table}: {len(mismatches)} value mismatches, e.g. {mismatches[:3]}'
    return got


def _assert_all_tables(bronze: Bronze):
    for table in TABLES:
        assert_matches_oracle(bronze, table)


def _by(rows, **flt):
    return [r for r in rows if all(r[k] == v for k, v in flt.items())]


def _merge(*parts: dict) -> dict:
    out: dict = {}
    for part in parts:
        for view, rows in part.items():
            out.setdefault(view, []).extend(rows)
    return out


# ---------------------------------------------------------------------------- saved pages

def _saved_views() -> dict:
    seasons, history = season_rows(PAGE_335, 335)
    # The 47 TopLists come without their league page: the category descriptors of the saved 335 page
    # stand in for 47's, so 'goals' joins a category and 'goal_assist' has none (LEFT JOIN miss).
    cats_47 = [dict(r, competition_id='47', source_season_key=KEY_47)
               for r in bundle_rows(PAGE_335, 335, KEY_335)['fotmob_leaderboard_categories_current']]
    return _merge(
        {'fotmob_competitions_current': competition_rows(CATALOG),
         'fotmob_competition_seasons_current': seasons,
         'fotmob_competition_season_history_current': history,
         'fotmob_leaderboard_categories_current': cats_47,
         'fotmob_leaderboards_current': (leaderboard_rows(TOP_GOALS, 'player', 47, KEY_47)
                                         + leaderboard_rows(TOP_ASSISTS, 'player', 47, KEY_47)
                                         + leaderboard_rows(TEAM_TOP, 'team', 47, KEY_47))},
        bundle_rows(PAGE_335, 335, KEY_335),
        bundle_rows(PAGE_8974, 8974, '2022'),
    )


@pytest.fixture(scope='module')
def saved() -> Bronze:
    return Bronze(**_saved_views())


# ---------------------------------------------------------------------------- static

def _text_of(table):
    return _sql_path(table).read_text(encoding='utf-8')


@pytest.mark.parametrize('table', TABLES)
def test_pure_select_sources_and_placeholder(table):
    text = _text_of(table)
    code = AUDIT.strip_sql(text)
    assert not re.search(r'\b(CREATE|INSERT|DELETE|MERGE|DROP|ALTER)\b', code, re.IGNORECASE)
    assert not re.search(r'WITH\s*\(\s*partitioning', code, re.IGNORECASE)
    refs = set(re.findall(r'iceberg\.\w+\.\w+', code))
    assert refs and refs <= BRONZE_SOURCES, refs
    assert set(re.findall(r'\{\{\s*(\w+)\s*\}\}', code)) == {PLACEHOLDER}
    on_own_line = re.findall(r'^[ \t]*\{\{ ' + PLACEHOLDER + r' \}\}[ \t]*$', text, re.MULTILINE)
    assert len(on_own_line) == len(re.findall(r'\{\{', code)) == len(refs)


def test_audit_layer_a_clean():
    cores = AUDIT.load_core_schemas()
    for table in TABLES:
        findings = AUDIT.audit_file(_sql_path(table), cores)
        assert not findings, (table, findings)


@pytest.mark.parametrize('table', TABLES)
def test_columns_order_and_types(saved, table):
    _, types = saved.run(table)
    expected = AUDIT.core_expected_columns(SCHEMA, table)
    assert [n for n, _ in types] == list(expected)
    assert {n: t for n, t in types} == {n: DUCKDB_TYPES[t] for n, t in expected.items()}


# ---------------------------------------------------------------------------- saved pages: values

@pytest.mark.parametrize('table', TABLES)
def test_saved_pages_match_oracle(saved, table):
    assert_matches_oracle(saved, table)


def test_saved_pages_row_counts(saved):
    counts = {t: len(saved.rows(t)) for t in TABLES}
    history = sum(1 for i in PAGE_335['seasons'] if i.get('seasonName') is not None)
    assert counts == {
        'fotmob_competitions': 5,               # 47, 112, 230, 335 + tombstone
        'fotmob_competition_seasons': len(PAGE_335['allAvailableSeasons']),
        'fotmob_competition_season_history': history,
        'fotmob_season_stages': 35 + 2,         # 335: 22 rounds + 12 stat links + 1 table; 8974: 2 rounds
        'fotmob_standings': 48,                 # 335: 12 teams x all/home/away/form
        'fotmob_season_teams': 12 + 4,
        'fotmob_leaderboards': 120 + 8 + 2,     # 47: goals + goal_assist (players) + 2 team rows
        'fotmob_playoff_brackets': 4,           # 8974: null slot is not a bronze row, {} is a TBD pair
    }


def test_standings_335_views_per_team(saved):
    rows = saved.rows('fotmob_standings')
    assert {(r['competition_id'], r['source_season_key'], r['table_id']) for r in rows} == {(335, KEY_335, 335)}
    per_type = {}
    for r in rows:
        per_type[r['table_type']] = per_type.get(r['table_type'], 0) + 1
    assert per_type == {'all': 12, 'home': 12, 'away': 12, 'form': 12}
    assert sorted(r['position'] for r in _by(rows, table_type='all')) == list(range(1, 13))
    table_stage, = _by(saved.rows('fotmob_season_stages'), stage_type='table')
    assert json.loads(table_stage['legend_json'])[0]['tKey'] == 'championship_playoff'


def test_playoff_8974_pairs(saved):
    rows = saved.rows('fotmob_playoff_brackets')
    assert {(r['stage_id'], r['matchup_key']): r['match_ids'] for r in rows} == {
        ('playoff:1/2', '-:tbd:tbd'): [],
        ('playoff:1/2', '2:164739:164720'): [4053616],
        ('playoff:1/2', '4:162196:4427'): [4053615],
        ('playoff:final', '-1:162196:164720'): [4053697],
    }
    assert all(r['_conform_issues_json'] is None for r in rows)
    stages = {r['stage_id']: r['participant_count'] for r in _by(saved.rows('fotmob_season_stages'), competition_id=8974)}
    assert stages == {'playoff:1/2': 4, 'playoff:final': 2}


def test_leaderboards_47_categories_left_join(saved):
    rows = saved.rows('fotmob_leaderboards')
    goals = _by(rows, stat_name='goals')
    assists = _by(rows, stat_name='goal_assist')
    assert len(goals) == 120 and len(assists) == 8
    assert {(r['category_header'], r['category'], r['category_order']) for r in goals} == {
        ('Top scorer', 'Top Stat', 1)}
    assert all(r['category'] is None and r['category_header'] is None and r['fetch_all_url'] is None
               for r in assists)
    top, = _by(goals, rank=1, top_list_index=0, source_item_index=0)
    assert (top['participant_id'], top['team_id'], top['participant_name']) == (737066, 8456, 'Erling Haaland')


def test_competitions_catalog(saved):
    rows = {r['competition_id']: r for r in saved.rows('fotmob_competitions')}
    assert rows[47]['has_catalog_conflict'] is True
    assert rows[TOMBSTONE_ID]['is_tombstoned'] is True and rows[TOMBSTONE_ID]['competition_name'] is None
    assert all(r['discovery_date'] == T0.date() for r in rows.values())
    assert json.loads(rows[47]['conflict_fields_json']) == ['country_name']


def test_competition_seasons_335_only_children(saved):
    rows = saved.rows('fotmob_competition_seasons')
    assert all(r['parent_season_key'] is None and r['season_part'] is None and r['has_child_seasons'] is False
               for r in rows)
    selected, = _by(rows, is_selected=True)
    assert selected['source_season_key'] == KEY_335 and selected['source_order'] == 0


# ---------------------------------------------------------------------------- scope placeholder

def test_scope_placeholder_filters_varchar_and_bigint_views(saved):
    # Decision 1: the scope is applied after competition_id is BIGINT, so one expression works for
    # varchar-id views (competitions, seasons, categories) and bigint-id views alike.
    for scope, comp in (('competition_id = 335', 335), ('competition_id = 47', 47)):
        for table in TABLES:
            full = [r for r in saved.rows(table) if r['competition_id'] == comp]
            assert sorted(map(str, saved.rows(table, scope))) == sorted(map(str, full)), (scope, table)
    lb = saved.rows('fotmob_leaderboards', "competition_id = 47 AND source_season_key = '2025/2026'")
    assert len(lb) == 130 and any(r['category'] for r in lb)


# ---------------------------------------------------------------------------- scenarios

def test_split_seasons_parent_and_children_distinguishable():
    page_112 = {
        'details': {'id': 112, 'selectedSeason': '2026 - Apertura', 'latestSeason': '2026 - Clausura'},
        'seostr': 'liga-profesional',
        'allAvailableSeasons': ['2026 - Clausura', '2026 - Apertura', '2026', '2025'],
        'seasons': [{'seasonName': '2026 - Apertura', 'winner': {'id': 10076, 'name': 'Platense'},
                     'loser': {'id': 9796, 'name': 'Huracan'}}],
        'stats': {'seasonStatLinks': [
            {'Name': '2026 - Apertura', 'TournamentId': 27420, 'StageId': 1, 'RelativePath': '/x/27420'},
            {'Name': '2025', 'TournamentId': '26101', 'StageId': 1}]},
    }
    page_230 = {'details': {'id': 230, 'selectedSeason': '2026/2027 - Apertura'},
                'allAvailableSeasons': ['2026/2027 - Apertura', '2025/2026 - Clausura']}
    s112, h112 = season_rows(page_112, 112)
    s230, h230 = season_rows(page_230, 230)
    bronze = Bronze(**_merge(
        {'fotmob_competition_seasons_current': s112 + s230,
         'fotmob_competition_season_history_current': h112 + h230,
         'fotmob_leaderboards_current': leaderboard_rows(TOP_ASSISTS, 'player', 112, '2026 - Clausura')},
        bundle_rows(rescoped(PAGE_335, 112, '2026 - Apertura'), 112, '2026 - Apertura'),
        bundle_rows(rescoped(PAGE_335, 112, '2026'), 112, '2026'),
    ))
    _assert_all_tables(bronze)
    seasons = {(r['competition_id'], r['source_season_key']): r for r in bronze.rows('fotmob_competition_seasons')}
    assert {k: (r['parent_season_key'], r['season_part'], r['has_child_seasons']) for k, r in seasons.items()} == {
        (112, '2026'): (None, None, True),
        (112, '2026 - Apertura'): ('2026', 'Apertura', False),
        (112, '2026 - Clausura'): ('2026', 'Clausura', False),
        (112, '2025'): (None, None, False),
        (230, '2026/2027 - Apertura'): (None, None, False),
        (230, '2025/2026 - Clausura'): (None, None, False),
    }
    assert seasons[(112, '2026 - Apertura')]['winner_team_id'] == 10076
    assert {k[1]: r['stat_tournament_id'] for k, r in seasons.items() if k[0] == 112} == {
        '2026': None, '2026 - Apertura': 27420, '2026 - Clausura': None, '2025': 26101}
    assert json.loads(seasons[(112, '2026 - Apertura')]['stat_link_json'])['RelativePath'] == '/x/27420'
    # Context tables stay under the key FotMob served them with — nothing is moved to a child/parent.
    for table, keys in (('fotmob_standings', {'2026', '2026 - Apertura'}),
                        ('fotmob_season_stages', {'2026', '2026 - Apertura'}),
                        ('fotmob_season_teams', {'2026', '2026 - Apertura'}),
                        ('fotmob_leaderboards', {'2026 - Clausura'})):
        assert {r['source_season_key'] for r in bronze.rows(table)} == keys, table
    assert len(_by(bronze.rows('fotmob_standings'), source_season_key='2026')) == 48
    assert len(_by(bronze.rows('fotmob_standings'), source_season_key='2026 - Apertura')) == 48


def test_latest_version_wins_and_wide_identity_collapses_deterministically():
    stages = bundle_rows(PAGE_335, 335, KEY_335)['fotmob_season_stages_current']
    newer = [dict(r, stage_name='renamed', _observed_at=T0 + timedelta(hours=2),
                  _ingested_at=T0 + timedelta(hours=2), _target_batch_id='new') for r in stages[:3]]
    goals = leaderboard_rows(TOP_GOALS, 'player', 47, KEY_47)[:5]
    # Same Silver key twice in one observation (bronze identity also has rank/top_list_index).
    twin = dict(goals[0], rank=7, top_list_index=1, stat_value=99.0)
    early_twin = dict(goals[1], rank=0, top_list_index=0, stat_value=55.0,
                      _observed_at=T0 - timedelta(hours=1), _ingested_at=T0 - timedelta(hours=1))
    standings = bundle_rows(PAGE_335, 335, KEY_335)['fotmob_standings_current'][:4]
    st_newer = dict(standings[0], points=99, _observed_at=T0 + timedelta(hours=1),
                    _ingested_at=T0 + timedelta(hours=1), _target_batch_id='new')
    # Same Silver key in one observation (bronze identity also has table_name/position).
    st_twin = dict(standings[1], position=standings[1]['position'] + 20, table_name='Zeta', points=-1)
    st_twin2 = dict(standings[1], table_name='Alpha', points=-2)
    playoff = bundle_rows(PAGE_8974, 8974, '2022')['fotmob_playoff_brackets_current']
    # Same matchup_key with other match_ids: larger batch wins, then smaller match_ids.
    pb_twin = dict(playoff[1], match_ids='[1]', _target_batch_id='zz')
    pb_twin2 = dict(playoff[1], match_ids='[0]', _target_batch_id='zz')
    bronze = Bronze(fotmob_season_stages_current=stages + newer,
                    fotmob_leaderboards_current=goals + [twin, early_twin],
                    fotmob_standings_current=standings + [st_newer, st_twin, st_twin2],
                    fotmob_playoff_brackets_current=playoff + [pb_twin, pb_twin2])
    _assert_all_tables(bronze)
    rows = {r['stage_id']: r for r in bronze.rows('fotmob_season_stages')}
    assert len(rows) == len(stages)
    assert {r['stage_id'] for r in rows.values() if r['_bronze_batch_id'] == 'new'} == {r['stage_id'] for r in newer}
    assert all(rows[r['stage_id']]['stage_name'] == 'renamed' for r in newer)
    lb = bronze.rows('fotmob_leaderboards')
    assert len(lb) == 5
    first, = _by(lb, participant_id=goals[0]['participant_id'])
    assert (first['rank'], first['top_list_index'], first['stat_value']) == (goals[0]['rank'], 0, goals[0]['stat_value'])
    second, = _by(lb, participant_id=goals[1]['participant_id'])
    assert second['stat_value'] == goals[1]['stat_value']
    st = {(r['table_type'], r['team_id']): r for r in bronze.rows('fotmob_standings')}
    assert len(st) == 4
    assert st[(standings[0]['table_type'], standings[0]['team_id'])]['points'] == 99
    assert st[(standings[1]['table_type'], standings[1]['team_id'])]['points'] == -2
    pb, = _by(bronze.rows('fotmob_playoff_brackets'), matchup_key='2:164739:164720')
    assert (pb['match_ids'], pb['_bronze_batch_id']) == ([0], 'zz')


def test_team_leaderboard_participant_zero_and_player_without_id_dropped():
    team = leaderboard_rows(TEAM_TOP, 'team', 47, KEY_47)
    assert all(r['participant_id'] is None for r in team)
    players = leaderboard_rows(TOP_ASSISTS, 'player', 47, KEY_47)
    bad = [dict(players[0], participant_id=None), dict(players[1], participant_id=0)]
    bronze = Bronze(fotmob_leaderboards_current=team + players[2:] + bad)
    _assert_all_tables(bronze)
    rows = bronze.rows('fotmob_leaderboards')
    assert {(r['participant_id'], r['team_id'], r['participant_name'])
            for r in _by(rows, participant_type='team')} == {(0, 9825, 'Arsenal'), (0, 8650, 'Liverpool')}
    assert len(_by(rows, participant_type='player')) == len(players) - 2


def test_duplicate_category_name_does_not_fan_out():
    cats = bundle_rows(PAGE_335, 335, KEY_335)['fotmob_leaderboard_categories_current']
    goals_cat, = [c for c in cats if c['name'] == 'goals' and c['participant_type'] == 'player']
    dup = dict(goals_cat, source_order=0, header='Goals (dup)')
    lb = leaderboard_rows(TOP_GOALS, 'player', 335, KEY_335) + leaderboard_rows(TOP_ASSISTS, 'player', 335, KEY_335)
    bronze = Bronze(fotmob_leaderboard_categories_current=cats + [dup], fotmob_leaderboards_current=lb)
    _assert_all_tables(bronze)
    rows = bronze.rows('fotmob_leaderboards')
    assert len(rows) == len(lb)
    assert {(r['category_header'], r['category_order']) for r in _by(rows, stat_name='goals')} == {('Goals (dup)', 0)}
    assert all(r['category_order'] is None for r in _by(rows, stat_name='goal_assist'))


def test_bad_optional_values_are_null_with_conform_issue():
    # Values chosen so Trino and DuckDB agree: 'x', 'n/a', 'bad', 'abc' fail both engines' casts;
    # participant_count 3.5 (DOUBLE) is rejected by the explicit integrality check in SQL rather than
    # by the engine cast (both engines would round 3.5); '-3' / 4.0 / '17' must parse.
    # The raw text of a DOUBLE in the issue is json_format(CAST(x AS JSON)): '3.5' in both Trino
    # (checked live 02.11; CAST(double AS VARCHAR) there gives '3.5E0') and DuckDB.
    views = bundle_rows(PAGE_335, 335, KEY_335)
    playoff = bundle_rows(PAGE_8974, 8974, '2022')['fotmob_playoff_brackets_current']
    standings = views['fotmob_standings_current']
    stages = views['fotmob_season_stages_current']
    competitions = competition_rows(CATALOG)
    standings[0]['deduction'], standings[1]['deduction'] = 'x', '-3'
    stages[0]['participant_count'], stages[1]['participant_count'] = 3.5, 4.0
    competitions[0]['discovery_date'] = 'n/a'
    playoff[1]['match_ids'], playoff[2]['source_stage_id'] = 'bad', 'abc'
    playoff[3]['source_stage_id'] = '17'
    bronze = Bronze(fotmob_standings_current=standings, fotmob_season_stages_current=stages,
                    fotmob_competitions_current=competitions, fotmob_playoff_brackets_current=playoff)
    _assert_all_tables(bronze)

    def issues(row):
        return None if row['_conform_issues_json'] is None else json.loads(row['_conform_issues_json'])

    st = {(r['table_type'], r['team_id']): r for r in bronze.rows('fotmob_standings')}
    bad_st = st[(standings[0]['table_type'], standings[0]['team_id'])]
    assert bad_st['deduction'] is None
    assert issues(bad_st) == [{'column': 'deduction', 'raw': 'x', 'reason': 'not an integer'}]
    assert st[(standings[1]['table_type'], standings[1]['team_id'])]['deduction'] == -3
    sg = {r['stage_id']: r for r in bronze.rows('fotmob_season_stages')}
    assert sg[stages[0]['stage_id']]['participant_count'] is None
    assert issues(sg[stages[0]['stage_id']]) == [
        {'column': 'participant_count', 'raw': '3.5', 'reason': 'not an integer'}]
    assert sg[stages[1]['stage_id']]['participant_count'] == 4 and issues(sg[stages[1]['stage_id']]) is None
    comp = {r['competition_id']: r for r in bronze.rows('fotmob_competitions')}
    bad_comp = comp[int(competitions[0]['competition_id'])]
    assert bad_comp['discovery_date'] is None
    assert [i['column'] for i in issues(bad_comp)] == ['discovery_date']
    pb = {r['matchup_key']: r for r in bronze.rows('fotmob_playoff_brackets')}
    assert pb['2:164739:164720']['match_ids'] is None
    assert issues(pb['2:164739:164720'])[0]['raw'] == 'bad'
    assert pb['4:162196:4427']['source_stage_id'] is None
    assert issues(pb['4:162196:4427'])[0]['column'] == 'source_stage_id'
    assert pb['-1:162196:164720']['source_stage_id'] == 17
    clean = [r for r in bronze.rows('fotmob_standings') if r is not bad_st]
    assert all(r['_conform_issues_json'] is None for r in clean)


def test_bad_keys_drop_only_their_rows():
    seasons, history = season_rows(PAGE_335, 335)
    competitions = competition_rows(CATALOG)
    views = bundle_rows(PAGE_335, 335, KEY_335)
    lb = leaderboard_rows(TOP_ASSISTS, 'player', 47, KEY_47)
    bad = {
        'fotmob_competitions_current': [dict(competitions[0], competition_id='abc'),
                                        dict(competitions[1], competition_id='')],
        'fotmob_competition_seasons_current': [dict(seasons[0], competition_id='abc'),
                                               dict(seasons[1], source_season_key='')],
        'fotmob_competition_season_history_current': [dict(history[0], competition_id='x335'),
                                                      dict(history[1], history_season_label='')],
        'fotmob_season_stages_current': [dict(views['fotmob_season_stages_current'][0], stage_id='',
                                              source_season_key='2099')],
        'fotmob_standings_current': [dict(views['fotmob_standings_current'][0], source_season_key='',
                                          team_id=1)],
        'fotmob_season_teams_current': [dict(views['fotmob_season_teams_current'][0], team_id=None,
                                             source_season_key='2099')],
        'fotmob_leaderboards_current': [dict(lb[0], stat_name='', participant_id=1),
                                        dict(lb[1], team_id=None, participant_id=2)],
        'fotmob_leaderboard_categories_current': [dict(views['fotmob_leaderboard_categories_current'][0],
                                                       competition_id='abc')],
    }
    good = _merge(
        {'fotmob_competitions_current': competitions[2:], 'fotmob_competition_seasons_current': seasons[2:],
         'fotmob_competition_season_history_current': history[2:], 'fotmob_leaderboards_current': lb},
        views)
    clean = Bronze(**good)
    dirty = Bronze(**_merge(good, bad))
    _assert_all_tables(dirty)
    for table in TABLES:
        assert sorted(map(str, dirty.rows(table))) == sorted(map(str, clean.rows(table))), table
