"""FotMob Silver entity family (#2019): the 6 ``dags/sql/silver/fotmob_*.sql.j2`` files for players,
player career, player market values, teams, team squads and transfers.

The SQL is Trino SQL (production). Here it is rendered with ``render_sql_template``, transpiled by
sqlglot to DuckDB (as in ``test_fotmob_season_silver_sql.py``) and executed over bronze ``_current``
rows built by the real FotMob code from saved pages (``tests/fixtures/fotmob_silver/entities``,
``tests/fixtures/fotmob/transfer_page_batch/league_130_page6.json``):
``FotMobIngestService._player_snapshot_row`` / ``_team_rows`` (team + squad of the same page),
``parse_transfers`` + the competition/event-year assembly of ``service.py``, then the repository
JSON encoding (``normalize_rows``). Bronze column types come from the bronze schema snapshot
(``tests/fixtures/bronze_schemas.json``).

Expected rows come from an independent Python oracle that reads every column through its ``from``
path in ``configs/medallion/fotmob_silver/core_schema.yaml`` (JSONPath with ``[N]``, ``[*]`` and
``[?(@.K=='V')]``); only ``derived:`` columns and the lead decisions of #2019 (free agent id 2 ->
NULL, transfer_at_utc only with a zone, career/market-value unrolling from the chosen player
snapshot, dedup tie-breaks) are spelled out here.
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
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

sqlglot = pytest.importorskip('sqlglot')
duckdb = pytest.importorskip('duckdb')

ROOT = Path(__file__).resolve().parents[3]
SILVER = ROOT / 'dags' / 'sql' / 'silver'
FIXTURES = ROOT / 'tests' / 'fixtures'
ENTITIES_DIR = FIXTURES / 'fotmob_silver' / 'entities'
SCHEMA_PATH = ROOT / 'configs' / 'medallion' / 'fotmob_silver' / 'core_schema.yaml'

_DAGS_DIR = ROOT / 'dags'
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))
os.environ.setdefault('MEDALLION_CONFIG_DIR', str(ROOT / 'configs' / 'medallion'))

from scrapers.fotmob.parsers import parse_transfers  # noqa: E402
from scrapers.fotmob.repository import normalize_rows  # noqa: E402
from scrapers.fotmob.service import FotMobIngestService, _event_year  # noqa: E402

pytestmark = pytest.mark.unit

TABLES = (
    'fotmob_players', 'fotmob_player_career', 'fotmob_player_market_values', 'fotmob_teams',
    'fotmob_team_squads', 'fotmob_transfers',
)
VIEWS = (
    'fotmob_player_snapshots_current', 'fotmob_team_snapshots_current',
    'fotmob_squad_snapshots_current', 'fotmob_transfer_events_current',
)
BRONZE_SOURCES = {f'iceberg.bronze.{v}' for v in VIEWS}
PLACEHOLDER = 'fotmob_entity_scope'

SNAPSHOT = json.loads((FIXTURES / 'bronze_schemas.json').read_text(encoding='utf-8'))['tables']
BRONZE_TYPES = {'varchar': 'VARCHAR', 'bigint': 'BIGINT', 'double': 'DOUBLE', 'boolean': 'BOOLEAN',
                'timestamp(6)': 'TIMESTAMP'}
DUCKDB_TYPES = {
    'bigint': 'BIGINT', 'integer': 'INTEGER', 'double': 'DOUBLE', 'boolean': 'BOOLEAN',
    'varchar': 'VARCHAR', 'date': 'DATE', 'decimal(38,8)': 'DECIMAL(38,8)', 'timestamp(6)': 'TIMESTAMP',
    'timestamp(6) with time zone': 'TIMESTAMP WITH TIME ZONE', 'array(varchar)': 'VARCHAR[]',
    'array(bigint)': 'BIGINT[]',
}
T0 = datetime(2026, 10, 1, 3, 0, 0)
PLAYER_ID = 789068
TEAM_ID = 8643
FREE_AGENT_ID = 2

# Dedup order (yaml dedup_order + lead decision 2 tie-breaks); NULLS LAST everywhere.
BASE_ORDER = (('_observed_at', 'desc'), ('_ingested_at', 'desc'), ('_target_batch_id', 'desc'))
TRANSFER_ORDER = BASE_ORDER + (('competition_id', 'asc'), ('source_page', 'asc'),
                               ('source_item_index', 'asc'))
ZONE = re.compile(r'(Z|[+-]\d{2}:?\d{2})$')
# Lead decision 9: transferType/role of a career entry — scalar -> text, object -> its JSON text.
OBJECT_AS_TEXT = {('fotmob_player_career', 'transfer_type'), ('fotmob_player_career', 'role')}
# Career dates also come as epoch milliseconds (JSON number or digit string; pilot 03.11, ~4 % of rows).
EPOCH_MS_DATES = {('fotmob_player_career', 'start_date'), ('fotmob_player_career', 'end_date')}


# ---------------------------------------------------------------------------- helpers

def _load_audit():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'audit_silver_charter_2019', ROOT / 'scripts' / 'audit_silver_charter.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


AUDIT = _load_audit()
SCHEMA = yaml.safe_load(SCHEMA_PATH.read_text(encoding='utf-8'))


def _sql_path(table: str) -> Path:
    return SILVER / f'{table}.sql.j2'


def _duckdb_sql(table: str, scope: str = 'TRUE') -> str:
    from utils.medallion_config import render_sql_template
    rendered = render_sql_template(_sql_path(table), fotmob_entity_scope=scope)
    sql = sqlglot.transpile(rendered, read='trino', write='duckdb')[0]
    return sql.replace('iceberg.bronze.', 'bronze.')


def _load_gz(name: str):
    with gzip.open(ENTITIES_DIR / name, 'rt', encoding='utf-8') as fh:
        return json.load(fh)


PLAYER_PAGE = _load_gz('player_789068.json.gz')
TEAM_PAGE = _load_gz('team_8643.json.gz')
TRANSFERS_P10 = _load_gz('transfers_130_page10.json.gz')
TRANSFERS_P6 = json.loads((FIXTURES / 'fotmob' / 'transfer_page_batch' / 'league_130_page6.json')
                          .read_text(encoding='utf-8'))


def _data(page: dict) -> dict:
    return page['pageProps']['data']


def player_page(player_id: int = PLAYER_ID, **changes) -> dict:
    """The saved player page, optionally re-identified / with top-level data fields replaced."""
    page = copy.deepcopy(PLAYER_PAGE)
    _data(page)['id'] = player_id
    _data(page).update(changes)
    return page


def team_page(team_id: int = TEAM_ID) -> dict:
    page = copy.deepcopy(TEAM_PAGE)
    page['details']['id'] = team_id
    return page


# ---------------------------------------------------------------------------- bronze rows

def _lineage(observed: datetime, batch: str, entity: str) -> dict:
    return {'_target_batch_id': batch, '_payload_sha256': None, '_parser_version': 'fotmob-native-v2',
            '_raw_uri': None, '_source': 'fotmob', '_entity_type': entity,
            '_observed_at': observed, '_ingested_at': observed + timedelta(minutes=5)}


def _rows(rows, entity: str, *, observed: datetime = T0, batch: str | None = None) -> list[dict]:
    out = []
    for row in normalize_rows(rows):
        row.update(_lineage(observed, batch or f'{entity}-{observed:%m%d%H%M}', entity))
        out.append(row)
    return out


def player_rows(page: dict | None = None, *, observed: datetime = T0, batch: str | None = None) -> dict:
    """fotmob_player_snapshots rows as FotMobIngestService writes them (service.py:3326)."""
    page = PLAYER_PAGE if page is None else page
    row = FotMobIngestService._player_snapshot_row(_data(page)['id'], page, observed)
    return {'fotmob_player_snapshots_current': _rows([row], 'player_snapshots', observed=observed, batch=batch)}


def team_rows(page: dict | None = None, *, observed: datetime = T0, batch: str | None = None) -> dict:
    """Team + squad rows of one /api/teams page (service.py:3050)."""
    page = TEAM_PAGE if page is None else page
    team, squad, _ = FotMobIngestService._team_rows(page['details']['id'], page, observed)
    return {'fotmob_team_snapshots_current': _rows([team], 'team_snapshots', observed=observed, batch=batch),
            'fotmob_squad_snapshots_current': _rows(squad, 'squad_snapshots', observed=observed, batch=batch)}


def transfer_rows(page: dict, competition_id: int | str = 130, *, observed: datetime = T0,
                  batch: str | None = None) -> dict:
    """One transfers page as service.py:2592-2601 assembles it."""
    rows = list(parse_transfers(page))
    for row in rows:
        row['competition_id'] = str(competition_id)
        row['event_year'] = _event_year(row.get('transfer_date'), observed.year)
    return {'fotmob_transfer_events_current': _rows(rows, 'transfer_events', observed=observed, batch=batch)}


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
    """DuckDB with the 4 ``bronze.fotmob_*_current`` views typed as in the bronze snapshot."""

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

_PATH_TOKEN = re.compile(
    r"\.([A-Za-z_][A-Za-z0-9_]*)|\[\"([^\"]+)\"\]|\[(\d+)\]|\[\?\(@\.(\w+)=='([^']*)'\)\]")


def jpath(obj, path: str):
    """Evaluate a simple JSONPath ('$', '.name', '["name"]', '[N]', "[?(@.K=='V')]" = first
    matching element); missing -> None."""
    assert path.startswith('$'), path
    rest, pos = path[1:], 0
    while pos < len(rest):
        m = _PATH_TOKEN.match(rest, pos)
        assert m, f'unsupported path {path}'
        pos = m.end()
        name = m.group(1) or m.group(2)
        if name is not None:
            obj = obj.get(name) if isinstance(obj, dict) else None
        elif m.group(3) is not None:
            idx = int(m.group(3))
            obj = obj[idx] if isinstance(obj, list) and idx < len(obj) else None
        else:
            key, value = m.group(4), m.group(5)
            hits = [e for e in obj if isinstance(e, dict) and e.get(key) == value] if isinstance(obj, list) else []
            obj = hits[0] if hits else None
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


INT_RANGE = {'integer': (-2 ** 31, 2 ** 31 - 1), 'bigint': (-2 ** 63, 2 ** 63 - 1)}


def _int_text(v: str, dtype: str):
    """conform: try integer from text: a number whose value is integral ('77', '77.0') -> int within
    the type range; anything else -> None (SQL: via DOUBLE + floor check, lesson 157)."""
    try:
        f = float(v)
    except ValueError:
        return None
    if not math.isfinite(f) or not f.is_integer():
        return None
    lo, hi = INT_RANGE[dtype]
    return int(f) if lo <= int(f) <= hi else None


def _date(v):
    """Lead decision 4: DATE from the first 10 characters (YYYY-MM-DD...)."""
    if isinstance(v, str) and re.match(r'\d{4}-\d{2}-\d{2}', v):
        try:
            return date.fromisoformat(v[:10])
        except ValueError:
            return None
    return None


def _ts(v):
    if not isinstance(v, str):
        return None
    try:
        parsed = datetime.fromisoformat(v.replace('Z', '+00:00'))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _career_date(v):
    """ISO text -> first 10 characters; epoch milliseconds (int, or 11-14 digits as text) -> UTC date."""
    if isinstance(v, int) and not isinstance(v, bool):
        v = str(v)
    if isinstance(v, str) and re.fullmatch(r'\d{11,14}', v):
        return datetime.fromtimestamp(int(v) / 1000, tz=timezone.utc).date()
    return _date(v)


def _decimal(v):
    return None if _text(v) is None else Decimal(_text(v))


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
    if dtype == 'decimal(38,8)':
        return _decimal(value)
    if dtype == 'timestamp(6) with time zone':
        return _ts(value)
    if dtype == 'varchar':
        return _text(value)
    raise AssertionError(dtype)


def _loads(raw):
    return None if raw is None else json.loads(raw)


def resolve(source: str, ctx: dict, element=None):
    """Value of a yaml ``from`` (non-derived) in the row context ``ctx`` (view -> bronze row); a path
    through ``[*]`` is evaluated on ``element`` (the unrolled array element of this Silver row)."""
    view, _, rest = source.partition('.')
    row = ctx.get(view) or {}
    if ':' not in rest:
        return row.get(rest)
    column, _, path = rest.partition(':')
    if '[*]' in path:
        return jpath(element, '$' + path.split('[*]', 1)[1])
    return jpath(_loads(row.get(column)), path)


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


def _view_rows(bronze: Bronze, view: str, **casts) -> list[dict]:
    """Bronze rows with the varchar ids cast once (TRY_CAST ... AS BIGINT)."""
    rows = []
    for r in bronze.views[view]:
        r = dict(r)
        for column in casts:
            r[column] = _int(r[column])
        rows.append(r)
    return rows


def _row(table: str, ctx: dict, derived: dict, *, element=None, extra_issues=()) -> dict:
    """One expected row: yaml ``from`` for non-derived columns, ``derived`` for the rest; conform
    ``try`` columns that fail -> NULL + issue (README: правило NULL)."""
    spec = _spec(table)
    main = ctx[spec['sources'][0]]
    out, issues = {}, list(extra_issues)
    for col in spec['columns']:
        name, dtype = col['name'], col['type'].lower()
        if name in derived:
            out[name] = derived[name]
            continue
        if str(col['from']).startswith('derived: NULL'):  # *_currency: FotMob sends no currency
            out[name] = None
            continue
        assert not str(col['from']).startswith('derived'), f'{table}.{name} needs a derived rule'
        raw = resolve(col['from'], ctx, element)
        if (table, name) in OBJECT_AS_TEXT and isinstance(raw, (dict, list)):
            out[name] = raw
            continue
        value = convert(raw, dtype)
        if (table, name) in EPOCH_MS_DATES:
            value = _career_date(raw)
        if col.get('conform') == 'try' and dtype in INT_RANGE and isinstance(raw, str):
            value = _int_text(raw, dtype)
        if col.get('conform') == 'try' and value is None and _valid(raw):
            issues.append({'column': name, 'raw': raw if isinstance(raw, str) else str(raw)})
        out[name] = value
    for col in spec.get('raw_json', []):
        raw = resolve(col['from'], ctx, element)
        out[col['name']] = raw if ':' in col['from'] else _loads(raw)
    out.update({
        '_bronze_batch_id': main['_target_batch_id'], '_bronze_observed_at': main['_observed_at'],
        '_bronze_ingested_at': main['_ingested_at'], '_parser_version': main['_parser_version'],
        '_conform_issues_json': issues or None,
    })
    return out


def _players(bronze: Bronze) -> list[dict]:
    rows = _view_rows(bronze, 'fotmob_player_snapshots_current', player_id=1)
    return _pick(rows, ('player_id',), BASE_ORDER)


def _section(raw):
    """A bronze JSON section; the 'null' literal and '' carry no section."""
    return None if raw in (None, '', 'null') else json.loads(raw)


def oracle(table: str, bronze: Bronze) -> list[dict]:
    spec = _spec(table)
    view = spec['sources'][0]
    out = []
    if table == 'fotmob_players':
        out = [_row(table, {view: r}, {}) for r in _players(bronze)]
    elif table == 'fotmob_player_career':
        for r in _players(bronze):
            items = (_section(r['career_history_json']) or {}).get('careerItems')
            for section, body in (items or {}).items():
                entries = body.get('teamEntries') if isinstance(body, dict) else None
                for index, entry in enumerate(entries or [], start=1):
                    out.append(_row(table, {view: r}, {'career_section': section, 'entry_index': index},
                                    element=entry))
    elif table == 'fotmob_player_market_values':
        for r in _players(bronze):
            seen = set()
            for element in (_section(r['market_values_json']) or {}).get('values') or []:
                row = _row(table, {view: r}, {}, element=element)
                if row['value_date'] is None or row['value_date'] in seen:
                    continue  # key error / later element of the same date
                seen.add(row['value_date'])
                out.append(row)
    elif table == 'fotmob_teams':
        rows = _view_rows(bronze, view, team_id=1)
        out = [_row(table, {view: r}, {}) for r in _pick(rows, ('team_id',), BASE_ORDER)]
    elif table == 'fotmob_team_squads':
        rows = _view_rows(bronze, view, team_id=1, member_id=1)
        out = [_row(table, {view: r}, {}) for r in _pick(rows, spec['natural_key'], BASE_ORDER)]
    elif table == 'fotmob_transfers':
        rows = _view_rows(bronze, view)
        feeds: dict = {}
        bad: dict = {}
        for r in rows:
            raw_competition = r['competition_id']
            r['competition_id'] = _int(raw_competition)
            if r['competition_id'] is not None:
                feeds.setdefault(r['transfer_event_id'], set()).add(r['competition_id'])
            elif _valid(raw_competition):
                bad.setdefault(r['transfer_event_id'], set()).add(raw_competition)
        for r in _pick(rows, ('transfer_event_id',), TRANSFER_ORDER):
            event = r['transfer_event_id']
            raw_date = r['transfer_date']
            zoned = isinstance(raw_date, str) and ZONE.search(raw_date) is not None
            at_utc = _ts(raw_date) if zoned else None
            issues = []
            if zoned and at_utc is None:
                issues.append({'column': 'transfer_at_utc', 'raw': raw_date})
            if bad.get(event):
                issues.append({'column': 'source_competition_ids', 'raw': ','.join(sorted(bad[event]))})
            out.append(_row(table, {view: r}, {
                'transfer_at_utc': at_utc,
                'from_team_id': None if r['from_club_id'] == FREE_AGENT_ID else r['from_club_id'],
                'to_team_id': None if r['to_club_id'] == FREE_AGENT_ID else r['to_club_id'],
                'source_competition_ids': sorted(feeds[event]) if feeds.get(event) else None,
            }, extra_issues=issues))
    else:
        raise AssertionError(table)
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
    if column.endswith('_json') or isinstance(want, (dict, list)) and dtype == 'varchar':
        return _close(None if got is None else json.loads(got), want)
    if dtype.startswith('decimal'):
        return got == want  # exact: Decimal equality is numeric, no float rounding
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


def _issues(row):
    return None if row['_conform_issues_json'] is None else json.loads(row['_conform_issues_json'])


# ---------------------------------------------------------------------------- saved pages

def _saved_views() -> dict:
    return _merge(player_rows(), team_rows(), transfer_rows(TRANSFERS_P10), transfer_rows(TRANSFERS_P6))


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
    assert refs == {f'iceberg.bronze.{v}' for v in _spec(table)['sources']}
    assert set(re.findall(r'\{\{\s*(\w+)\s*\}\}', code)) == {PLACEHOLDER}
    on_own_line = re.findall(r'^[ \t]*\{\{ ' + PLACEHOLDER + r' \}\}[ \t]*$', text, re.MULTILINE)
    assert len(on_own_line) == len(re.findall(r'\{\{', code)) == 1


_DATE_CAST = re.compile(r'\bAS\s+DATE\s*\)', re.IGNORECASE)
_DATE_FROM_PREFIX = re.compile(r'TRY_CAST\(\s*substr\(\s*[\w.]+\s*,\s*1\s*,\s*10\s*\)\s+AS\s+DATE\s*\)',
                               re.IGNORECASE)


_DATE_FROM_EPOCH_MS = re.compile(
    r"CAST\(\s*from_unixtime\(\s*CAST\(\s*\w+\.(start_date|end_date)_raw\s+AS\s+DOUBLE\s*\)\s*/\s*1000\s*\)"
    r"\s+AT\s+TIME\s+ZONE\s+'UTC'\s+AS\s+DATE\s*\)", re.IGNORECASE)


@pytest.mark.parametrize('table', TABLES)
def test_dates_cast_from_first_10_characters(table):
    # Lead decision 4. DuckDB's TRY_CAST('2016-12-31T00:00:00+00:00' AS DATE) succeeds, Trino's is
    # NULL — so the data tests cannot catch a cast of the whole string; the text check does.
    # The only other DATE cast allowed: career start/end_date from epoch milliseconds, in UTC.
    code = AUDIT.strip_sql(_text_of(table))
    epoch = _DATE_FROM_EPOCH_MS.findall(code)
    assert sorted(epoch) == (['end_date', 'start_date'] if table == 'fotmob_player_career' else [])
    assert len(_DATE_CAST.findall(code)) == len(_DATE_FROM_PREFIX.findall(code)) + len(epoch)


@pytest.mark.parametrize('field, column', (('transferType', 'transfer_type'), ('role', 'role')))
def test_career_object_text_branch(field, column):
    # Lead decision 9. Trino's json_extract_scalar on an object is NULL (DuckDB's ->> returns the
    # object text), so the json_format(json_extract(...)) branch is what keeps an object in Trino.
    code = AUDIT.strip_sql(_text_of('fotmob_player_career'))
    m = re.search(r'COALESCE\((.*?)\)\s+AS\s+' + column + r'\b', code, re.DOTALL)
    assert m, column
    assert re.search(r"json_extract_scalar\(\s*\w+\.entry_j\s*,\s*'\$\." + field + r"'\s*\)", m.group(1))
    assert re.search(r"json_format\(\s*json_extract\(\s*\w+\.entry_j\s*,\s*'\$\." + field + r"'\s*\)\s*\)",
                     m.group(1))


def test_audit_layer_a_clean():
    cores = AUDIT.load_core_schemas()
    for table in TABLES:
        findings = AUDIT.audit_file(_sql_path(table), cores)
        assert not findings, (table, findings)


def test_all_task_2019_tables_have_sql():
    assert {t for t, s in SCHEMA['tables'].items() if s.get('task') == 2019} == set(TABLES)
    assert all(_spec(t)['scope'] == 'global' and _spec(t)['partitioning'] == [] for t in TABLES)


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
    assert counts == {
        'fotmob_players': 1,
        'fotmob_player_career': 7,           # senior 5 + national team 2 teamEntries
        'fotmob_player_market_values': 69,
        'fotmob_teams': 1,
        'fotmob_team_squads': 30,            # 29 players + 1 coach (5 sections)
        'fotmob_transfers': 100,             # two pages of league 130, no shared events
    }


def test_player_789068_card(saved):
    p, = saved.rows('fotmob_players')
    assert (p['player_id'], p['player_name'], p['birth_date'], p['contract_end']) == (
        PLAYER_ID, 'Esequiel Barco', date(1999, 3, 29), date(2027, 6, 30))
    assert (p['height_cm'], p['shirt_number'], p['preferred_foot'], p['country_code']) == (167, 5, 'right', 'ARG')
    assert (p['status'], p['data_provider'], p['is_on_loan'], p['primary_position_key']) == (
        'active', 'enet', False, 'centerattackingmidfielder')
    assert p['is_coach'] is False and p['snapshot_date'] == T0.date()
    # 'null' sections of the page (injuryInformation, coachStats, internationalDuty) -> NULL, no issue.
    assert p['injury_information_json'] is None and p['coach_stats_json'] is None
    assert p['international_duty_json'] is None
    assert p['injury_key'] is None and p['injury_name'] is None and p['injury_expected_return'] is None
    assert p['_conform_issues_json'] is None


def test_career_789068_sections_and_entries(saved):
    rows = saved.rows('fotmob_player_career')
    assert sorted((r['career_section'], r['entry_index']) for r in rows) == sorted(
        [('senior', i) for i in range(1, 6)] + [('national team', i) for i in range(1, 3)])
    first, = _by(rows, career_section='senior', entry_index=1)
    assert (first['team_id'], first['team_name'], first['start_date'], first['end_date'], first['is_active']) == (
        TEAM_ID, 'Spartak Moscow', date(2024, 7, 25), None, True)
    assert (first['appearances'], first['goals'], first['assists']) == (77, 23, 16)
    assert first['transfer_type'] is None and first['role'] is None and first['has_uncertain_data'] is False
    u20, = _by(rows, career_section='national team', entry_index=2)
    assert (u20['team_id'], u20['end_date'], u20['appearances']) == (5860, date(2019, 6, 30), 12)
    assert all(r['_conform_issues_json'] is None for r in rows)


def test_market_values_exact_decimal(saved):
    rows = saved.rows('fotmob_player_market_values')
    assert {r['currency'] for r in rows} == {'EUR'}
    assert len({r['value_date'] for r in rows}) == 69
    raw_dates = [v['date'] for v in _data(PLAYER_PAGE)['marketValues']['values']]
    assert raw_dates[0] == '2016-12-31T00:00:00+00:00'
    assert sorted(r['value_date'] for r in rows) == sorted(date.fromisoformat(d[:10]) for d in raw_dates)
    assert all(type(r['value_date']) is date for r in rows)
    top = max(rows, key=lambda r: r['market_value'])
    assert top['market_value'] == Decimal('17894659') and str(top['market_value']) == '17894659.00000000'
    first, = _by(rows, value_date=date(2016, 12, 31))
    assert (first['market_value'], first['lower_bound'], first['upper_bound']) == (
        Decimal('8260000'), Decimal('6190000'), Decimal('10320000'))
    assert (first['team_id'], first['team_name'], first['value_source'], first['is_period_start']) == (
        10078, 'Independiente', 'scisports', True)
    assert sum(1 for r in rows if r['market_value'] >= Decimal('1e7')) >= 2


def test_team_and_squad_8643(saved):
    t, = saved.rows('fotmob_teams')
    assert (t['team_id'], t['team_name'], t['short_name'], t['country_code'], t['gender'], t['team_type']) == (
        TEAM_ID, 'Spartak Moscow', 'Spartak', 'RUS', 'male', 'team')
    assert (t['primary_league_id'], t['primary_league_name'], t['latest_season']) == (63, 'Premier League', '2026/2027')
    assert (t['venue_name'], t['venue_city'], t['venue_latitude'], t['venue_longitude']) == (
        'Lukoil Arena', 'Moskva', 55.817875386, 37.440323731)
    # Lead decision 10: bronze reads paths FotMob does not send -> NULL, no issue.
    assert t['venue_capacity'] is None and t['page_url'] is None and t['_conform_issues_json'] is None
    squad = saved.rows('fotmob_team_squads')
    assert {mt: len(_by(squad, member_type=mt)) for mt in ('player', 'coach')} == {'player': 29, 'coach': 1}
    coach, = _by(squad, member_type='coach')
    assert (coach['member_id'], coach['section'], coach['section_index'], coach['member_index'], coach['role_key']) == (
        638888, 'coach', 0, 0, 'coach')
    assert sorted({r['section_index'] for r in squad}) == [0, 1, 2, 3, 4]
    assert all(r['position_description_json'] is None and r['transfer_value_currency'] is None for r in squad)
    zinkovskiy, = _by(squad, member_id=785808)
    assert (zinkovskiy['position_id'], zinkovskiy['position_ids_desc'], zinkovskiy['height_cm'], zinkovskiy['age'],
            zinkovskiy['transfer_value'], zinkovskiy['date_of_birth']) == (
        3, 'LW,LM', 175, 30, Decimal('1199437'), date(1996, 4, 14))


def test_transfers_saved_pages(saved):
    rows = saved.rows('fotmob_transfers')
    raw = TRANSFERS_P10['transfers'] + TRANSFERS_P6['transfers']
    assert len({r['transfer_event_id'] for r in rows}) == len(rows) == 100
    assert all(r['source_competition_ids'] == [130] for r in rows)
    assert all(r['transfer_at_utc'] is not None and r['_conform_issues_json'] is None for r in rows)
    assert all(r['transfer_date'] == date.fromisoformat(r['transfer_date_raw'][:10]) for r in rows)
    # Lead decision 3: free agent (club id 2) is not a team — id NULL, name kept.
    from_free = _by(rows, from_team_name='Free agent')
    to_free = _by(rows, to_team_name='Free agent')
    assert len(from_free) == sum(1 for t in raw if t.get('fromClubId') == FREE_AGENT_ID) >= 2
    assert len(to_free) == sum(1 for t in raw if t.get('toClubId') == FREE_AGENT_ID) >= 1
    assert all(r['from_team_id'] is None and r['from_team_full_name'] == 'Free agent' for r in from_free)
    assert all(r['to_team_id'] is None for r in to_free)
    assert not any(r['from_team_id'] == FREE_AGENT_ID or r['to_team_id'] == FREE_AGENT_ID for r in rows)
    # NULL money stays NULL — never a zero.
    assert sum(1 for r in rows if r['fee_value'] is None) == sum(
        1 for t in raw if (t.get('fee') or {}).get('value') is None and t.get('amountEuroEstimated') is None)
    assert sum(1 for r in rows if r['market_value'] is None) == sum(1 for t in raw if t.get('marketValue') is None)
    assert not any(r['fee_value'] == 0 or r['market_value'] == 0 for r in rows)
    assert all(r['money_currency'] is None for r in rows)


# ---------------------------------------------------------------------------- scope placeholder

def test_scope_placeholder_filters():
    """Scenario 8: one expression per file, over the cast key."""
    other_events = transfer_rows(TRANSFERS_P6)['fotmob_transfer_events_current']
    event_id = other_events[0]['transfer_event_id']
    bronze = Bronze(**_merge(player_rows(), player_rows(player_page(1000001)), team_rows(),
                             team_rows(team_page(9999)), transfer_rows(TRANSFERS_P10),
                             {'fotmob_transfer_events_current': other_events}))
    _assert_all_tables(bronze)
    for table, scope, key, value in (
            ('fotmob_players', f'player_id = {PLAYER_ID}', 'player_id', PLAYER_ID),
            ('fotmob_player_career', f'player_id = {PLAYER_ID}', 'player_id', PLAYER_ID),
            ('fotmob_player_market_values', f'player_id = {PLAYER_ID}', 'player_id', PLAYER_ID),
            ('fotmob_teams', f'team_id = {TEAM_ID}', 'team_id', TEAM_ID),
            ('fotmob_team_squads', f'team_id = {TEAM_ID}', 'team_id', TEAM_ID),
            ('fotmob_transfers', f"transfer_event_id = '{event_id}'", 'transfer_event_id', event_id)):
        full = bronze.rows(table)
        assert len({r[key] for r in full}) > 1, table
        scoped = bronze.rows(table, scope)
        assert scoped and sorted(map(str, scoped)) == sorted(map(str, _by(full, **{key: value}))), table


# ---------------------------------------------------------------------------- scenarios

def test_latest_snapshot_wins_children_from_it():
    """Scenario 1: two observations of the player/team/squad — the later one wins; career and market
    values come only from the chosen player snapshot."""
    newer_page = player_page(name='Renamed')
    items = _data(newer_page)['careerHistory']['careerItems']
    items['senior']['teamEntries'] = items['senior']['teamEntries'][:3]
    _data(newer_page)['marketValues']['values'] = _data(newer_page)['marketValues']['values'][-10:]
    newer_team = team_page()
    newer_team['details']['name'] = 'Spartak (new)'
    newer_team['squad']['squad'][1]['members'][0]['name'] = 'Keeper (new)'
    later = T0 + timedelta(hours=2)
    bronze = Bronze(**_merge(player_rows(), player_rows(newer_page, observed=later, batch='new'),
                             team_rows(), team_rows(newer_team, observed=later, batch='new')))
    _assert_all_tables(bronze)
    p, = bronze.rows('fotmob_players')
    assert (p['player_name'], p['_bronze_batch_id']) == ('Renamed', 'new')
    career = bronze.rows('fotmob_player_career')
    assert len(career) == 3 + 2 and {r['_bronze_batch_id'] for r in career} == {'new'}
    values = bronze.rows('fotmob_player_market_values')
    assert len(values) == 10 and {r['_bronze_batch_id'] for r in values} == {'new'}
    t, = bronze.rows('fotmob_teams')
    assert (t['team_name'], t['_bronze_batch_id']) == ('Spartak (new)', 'new')
    squad = bronze.rows('fotmob_team_squads')
    assert len(squad) == 30 and {r['_bronze_batch_id'] for r in squad} == {'new'}
    assert _by(squad, section='keepers', member_index=0)[0]['member_name'] == 'Keeper (new)'


def test_same_observation_larger_batch_wins():
    """Lead decision 2: equal _observed_at and _ingested_at -> the larger _target_batch_id wins."""
    p6 = copy.deepcopy(TRANSFERS_P6)
    p6['transfers'] = p6['transfers'][:3]
    older = [dict(r, player_name='Batch A')
             for r in transfer_rows(p6, batch='batch-a')['fotmob_transfer_events_current']]
    for later_first in (False, True):  # insertion order must not matter
        parts = [player_rows(player_page(name='Batch A'), batch='batch-a'),
                 player_rows(player_page(name='Batch B'), batch='batch-b'),
                 {'fotmob_transfer_events_current': older},
                 transfer_rows(p6, batch='batch-b')]
        bronze = Bronze(**_merge(*(parts[::-1] if later_first else parts)))
        _assert_all_tables(bronze)
        p, = bronze.rows('fotmob_players')
        assert (p['player_name'], p['_bronze_batch_id']) == ('Batch B', 'batch-b')
        assert {r['_bronze_batch_id'] for r in bronze.rows('fotmob_player_career')} == {'batch-b'}
        transfers = bronze.rows('fotmob_transfers')
        assert len(transfers) == 3
        assert {(r['_bronze_batch_id'], r['player_name'] == 'Batch A') for r in transfers} == {('batch-b', False)}


def test_transfer_event_in_two_feeds_is_one_row():
    """Scenario 2: the same event in the feeds of 130 and 47 -> one row, both competitions; an
    unparseable competition_id stays out of the array and is reported."""
    p130 = transfer_rows(TRANSFERS_P6)['fotmob_transfer_events_current']
    p47 = transfer_rows(TRANSFERS_P6, 47, observed=T0 + timedelta(hours=1), batch='feed-47')
    bad = dict(p130[0], competition_id='abc')
    bronze = Bronze(**_merge({'fotmob_transfer_events_current': p130 + [bad]}, p47))
    _assert_all_tables(bronze)
    rows = {r['transfer_event_id']: r for r in bronze.rows('fotmob_transfers')}
    assert len(rows) == len(p130)
    assert all(r['source_competition_ids'] == [47, 130] for r in rows.values())
    assert {r['_bronze_batch_id'] for r in rows.values()} == {'feed-47'}
    flagged = rows[p130[0]['transfer_event_id']]
    assert _issues(flagged) == [{'column': 'source_competition_ids', 'raw': 'abc',
                                 'reason': 'not an integer competition id'}]
    assert all(_issues(r) is None for k, r in rows.items() if k != p130[0]['transfer_event_id'])
    # Same observation and batch: the smaller competition_id breaks the tie (player_name is outside
    # the bronze event hash, so the feeds may differ there).
    tie_47 = [dict(r, player_name='From 47')
              for r in transfer_rows(TRANSFERS_P6, 47, batch='b')['fotmob_transfer_events_current']]
    tie = Bronze(**_merge(transfer_rows(TRANSFERS_P6, 130, batch='b'), {'fotmob_transfer_events_current': tie_47}))
    _assert_all_tables(tie)
    assert {r['player_name'] for r in tie.rows('fotmob_transfers')} == {'From 47'}


def test_free_agent_id_is_null_name_kept():
    """Scenario 3."""
    page = copy.deepcopy(TRANSFERS_P6)
    page['transfers'] = page['transfers'][:2]
    page['transfers'][0].update(fromClubId=2, fromClub='Free agent', fromClubFullName='Free agent')
    page['transfers'][1].update(toClubId=2, toClub='Free agent', toClubFullName='Free agent')
    bronze = Bronze(**transfer_rows(page))
    _assert_all_tables(bronze)
    got = {(r['from_team_id'], r['from_team_name'], r['to_team_id'], r['to_team_name'])
           for r in bronze.rows('fotmob_transfers')}
    t0, t1 = page['transfers']
    assert got == {(None, 'Free agent', t0['toClubId'], t0['toClub']),
                   (t1['fromClubId'], t1['fromClub'], None, 'Free agent')}


def test_transfer_date_zone_and_null_money():
    """Scenario 4: without a zone -> transfer_date only, no issue; garbage -> both NULL + issue;
    an offset is honoured; fee/marketValue null stay NULL."""
    page = copy.deepcopy(TRANSFERS_P6)
    page['transfers'] = page['transfers'][:4]
    page['transfers'][0].update(transferDate='2026-01-01T00:00:00', fee=None, marketValue=None,
                                amountEuroEstimated=None)
    page['transfers'][1]['transferDate'] = 'n/a'
    page['transfers'][2]['transferDate'] = '2026-01-01T00:30:00+02:00'
    bronze = Bronze(**transfer_rows(page))
    _assert_all_tables(bronze)
    rows = {r['transfer_date_raw']: r for r in bronze.rows('fotmob_transfers')}
    naive = rows['2026-01-01T00:00:00']
    assert (naive['transfer_date'], naive['transfer_at_utc'], naive['_conform_issues_json']) == (
        date(2026, 1, 1), None, None)
    assert naive['fee_value'] is None and naive['market_value'] is None
    garbage = rows['n/a']
    assert garbage['transfer_date'] is None and garbage['transfer_at_utc'] is None
    assert _issues(garbage) == [{'column': 'transfer_date', 'raw': 'n/a', 'reason': 'not an ISO date'}]
    offset = rows['2026-01-01T00:30:00+02:00']
    assert offset['transfer_date'] == date(2026, 1, 1)
    assert offset['transfer_at_utc'] == datetime(2025, 12, 31, 22, 30, tzinfo=timezone.utc)
    assert offset['_conform_issues_json'] is None


def test_bad_keys_drop_only_their_rows():
    """Scenario 5: unparseable/empty keys and a values[] element without a date."""
    good = _saved_views()
    player = good['fotmob_player_snapshots_current'][0]
    bad = {
        'fotmob_player_snapshots_current': [dict(player, player_id='abc'), dict(player, player_id='')],
        'fotmob_team_snapshots_current': [dict(good['fotmob_team_snapshots_current'][0], team_id='x8643')],
        'fotmob_squad_snapshots_current': [
            dict(good['fotmob_squad_snapshots_current'][0], member_id=''),
            dict(good['fotmob_squad_snapshots_current'][1], member_type='', member_id='1'),
            dict(good['fotmob_squad_snapshots_current'][2], team_id='abc', member_id='2')],
        'fotmob_transfer_events_current': [dict(good['fotmob_transfer_events_current'][0], transfer_event_id='')],
    }
    clean = Bronze(**good)
    dirty = Bronze(**_merge(good, bad))
    _assert_all_tables(dirty)
    for table in TABLES:
        assert sorted(map(str, dirty.rows(table))) == sorted(map(str, clean.rows(table))), table
    page = player_page()
    values = _data(page)['marketValues']['values']
    values[0].pop('date')
    values[1]['date'] = 'soon'
    values[3]['date'] = values[2]['date']  # two estimates on one date -> the earlier element
    no_date = Bronze(**player_rows(page))
    _assert_all_tables(no_date)
    rows = no_date.rows('fotmob_player_market_values')
    assert len(rows) == 66 and date(2016, 12, 31) not in {r['value_date'] for r in rows}
    twin, = _by(rows, value_date=date.fromisoformat(values[2]['date'][:10]))
    assert twin['market_value'] == Decimal(values[2]['value'])
    assert len(no_date.rows('fotmob_players')) == 1 and len(no_date.rows('fotmob_player_career')) == 7


def test_bad_optional_values_are_null_with_conform_issue():
    """Scenario 6: conform: try failures -> NULL + issue with the raw text; 'null' sections -> NULL
    raw, NULL scalars, no issue."""
    page = player_page(contractEnd='soon', marketValues=None, injuryInformation=None, coachStats=None,
                       birthDate={'utcTime': 'n/a', 'timezone': 'UTC'})
    entry = _data(page)['careerHistory']['careerItems']['senior']['teamEntries'][0]
    entry.update(appearances='abc', goals='1.5', assists='4.0', startDate='n/a', endDate='x')
    team = team_page()
    team['overview']['venue']['widget'].update(capacity='n/a', location=['x', 'x'])
    members = team['squad']['squad'][1]['members']
    members[0].update(shirtNumber=7.5, height=180.5, dateOfBirth='n/a')
    members[0].pop('dateOfBirthUTC', None)
    members[1].update(shirtNumber=12.0, height=181)
    views = _merge(player_rows(page), team_rows(team))
    views['fotmob_player_snapshots_current'][0]['snapshot_date'] = 'bad'
    views['fotmob_team_snapshots_current'][0]['snapshot_date'] = 'bad'
    keeper, = [r for r in views['fotmob_squad_snapshots_current']
               if r['section'] == 'keepers' and r['member_index'] == 0]
    keeper['snapshot_date'] = 'bad'
    bronze = Bronze(**views)
    _assert_all_tables(bronze)
    p, = bronze.rows('fotmob_players')
    assert (p['contract_end'], p['birth_date'], p['snapshot_date']) == (None, None, None)
    assert sorted(_issues(p), key=lambda i: i['column']) == [
        {'column': 'birth_date', 'raw': 'n/a', 'reason': 'not an ISO date'},
        {'column': 'contract_end', 'raw': 'soon', 'reason': 'not an ISO date'},
        {'column': 'snapshot_date', 'raw': 'bad', 'reason': 'not an ISO date'}]
    assert p['injury_information_json'] is None and p['coach_stats_json'] is None and p['injury_key'] is None
    assert bronze.rows('fotmob_player_market_values') == []
    c, = _by(bronze.rows('fotmob_player_career'), career_section='senior', entry_index=1)
    assert (c['appearances'], c['goals'], c['assists'], c['start_date'], c['end_date']) == (None, None, 4, None, None)
    assert {i['column']: i['raw'] for i in _issues(c)} == {
        'appearances': 'abc', 'goals': '1.5', 'start_date': 'n/a', 'end_date': 'x'}
    t, = bronze.rows('fotmob_teams')
    assert (t['venue_capacity'], t['venue_latitude'], t['venue_longitude'], t['snapshot_date']) == (
        None, None, None, None)
    assert {i['column']: i['raw'] for i in _issues(t)} == {
        'venue_capacity': 'n/a', 'venue_latitude': 'x', 'venue_longitude': 'x', 'snapshot_date': 'bad'}
    squad = {r['member_index']: r for r in _by(bronze.rows('fotmob_team_squads'), section='keepers')}
    assert (squad[0]['shirt_number'], squad[0]['height_cm'], squad[0]['date_of_birth'], squad[0]['snapshot_date']) == (
        None, None, None, None)
    assert {i['column']: i['raw'] for i in _issues(squad[0])} == {
        'shirt_number': '7.5', 'height_cm': '180.5', 'date_of_birth': 'n/a', 'snapshot_date': 'bad'}
    assert (squad[1]['shirt_number'], squad[1]['height_cm'], squad[1]['_conform_issues_json']) == (12, 181, None)
    assert all(r['position_description_json'] is None for r in squad.values())


def test_missing_children_leave_player_intact():
    """Scenario 7: no careerItems / empty teamEntries / SQL NULL market values -> no child rows."""
    no_items = player_page(1000001)
    _data(no_items)['careerHistory'].pop('careerItems')
    empty_entries = player_page(1000002)
    for body in _data(empty_entries)['careerHistory']['careerItems'].values():
        body['teamEntries'] = []
    sql_null = player_rows(player_page(1000003))
    sql_null['fotmob_player_snapshots_current'][0]['market_values_json'] = None
    empty = player_rows(player_page(1000004))
    empty['fotmob_player_snapshots_current'][0]['market_values_json'] = ''
    bronze = Bronze(**_merge(player_rows(no_items), player_rows(empty_entries), sql_null, empty))
    _assert_all_tables(bronze)
    assert {r['player_id'] for r in bronze.rows('fotmob_players')} == {1000001, 1000002, 1000003, 1000004}
    assert {r['player_id'] for r in bronze.rows('fotmob_player_career')} == {1000003, 1000004}
    assert {r['player_id'] for r in bronze.rows('fotmob_player_market_values')} == {1000001, 1000002}


def test_transfer_type_object_and_coach_card():
    """Scenario 9: a career transferType object -> its JSON text; a coach card in fotmob_players."""
    page = player_page()
    senior = _data(page)['careerHistory']['careerItems']['senior']['teamEntries']
    senior[0]['transferType'] = {'localizationKey': 'free_transfer', 'text': 'free transfer'}
    senior[1]['transferType'] = 'loan'
    senior[1]['role'] = 'captain'
    coach = player_page(1000009, isCoach=True, name='Some Coach', marketValues=None)
    bronze = Bronze(**_merge(player_rows(page), player_rows(coach)))
    _assert_all_tables(bronze)
    career = {r['entry_index']: r for r in _by(bronze.rows('fotmob_player_career'), player_id=PLAYER_ID,
                                                 career_section='senior')}
    assert json.loads(career[1]['transfer_type']) == {'localizationKey': 'free_transfer', 'text': 'free transfer'}
    assert (career[2]['transfer_type'], career[2]['role']) == ('loan', 'captain')
    coach_row, = _by(bronze.rows('fotmob_players'), player_id=1000009)
    assert coach_row['is_coach'] is True and coach_row['player_name'] == 'Some Coach'


def test_career_dates_from_epoch_milliseconds():
    """Career start/end dates sent as epoch milliseconds (JSON number or digit string) -> UTC date,
    no issue; other text still -> NULL + issue."""
    page = player_page()
    senior = _data(page)['careerHistory']['careerItems']['senior']['teamEntries']
    senior[0].update(startDate=1719792000000, endDate='1767139200000')
    senior[1].update(startDate='x', endDate=1767225599999)   # 2025-12-31T23:59:59.999Z
    bronze = Bronze(**player_rows(page))
    _assert_all_tables(bronze)
    career = {r['entry_index']: r for r in _by(bronze.rows('fotmob_player_career'), career_section='senior')}
    assert (career[1]['start_date'], career[1]['end_date'], career[1]['_conform_issues_json']) == (
        date(2024, 7, 1), date(2025, 12, 31), None)
    assert (career[2]['start_date'], career[2]['end_date']) == (None, date(2025, 12, 31))
    assert [(i['column'], i['raw']) for i in _issues(career[2])] == [('start_date', 'x')]
