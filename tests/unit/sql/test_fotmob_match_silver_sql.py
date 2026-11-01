"""FotMob Silver match family (#2017): the 8 ``dags/sql/silver/fotmob_*.sql.j2`` files.

The SQL is Trino SQL (production). Here it is rendered with ``render_sql_template``, transpiled by
sqlglot to DuckDB (as in ``test_transfermarkt_native_v2_sql.py``) and executed over bronze rows built
from 23 saved FotMob cards (``tests/fixtures/fotmob_silver/cards``) by the real
``FotMobIngestService._match_payload_row`` + repository JSON encoding.

Expected rows come from an independent Python oracle that reads every column through its ``from``
path in ``configs/medallion/fotmob_silver/core_schema.yaml``; only ``derived:`` columns and the two
lead decisions (lineage/calendar fallback of ``fotmob_matches``, COALESCE in ``fotmob_match_teams``)
are spelled out here.
"""
from __future__ import annotations

import copy
import gzip
import json
import math
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

sqlglot = pytest.importorskip('sqlglot')
duckdb = pytest.importorskip('duckdb')

ROOT = Path(__file__).resolve().parents[3]
SILVER = ROOT / 'dags' / 'sql' / 'silver'
CARDS_DIR = ROOT / 'tests' / 'fixtures' / 'fotmob_silver' / 'cards'
SCHEMA_PATH = ROOT / 'configs' / 'medallion' / 'fotmob_silver' / 'core_schema.yaml'

_DAGS_DIR = ROOT / 'dags'
if str(_DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(_DAGS_DIR))
os.environ.setdefault('MEDALLION_CONFIG_DIR', str(ROOT / 'configs' / 'medallion'))

from scrapers.fotmob.repository import normalize_rows  # noqa: E402
from scrapers.fotmob.service import FotMobIngestService  # noqa: E402

pytestmark = pytest.mark.unit

TABLES = (
    'fotmob_matches', 'fotmob_match_teams', 'fotmob_match_team_stats', 'fotmob_match_lineups',
    'fotmob_match_player_stats', 'fotmob_match_events', 'fotmob_shots', 'fotmob_match_momentum',
)
BRONZE_SOURCES = {
    'iceberg.bronze.fotmob_matches_current',
    'iceberg.bronze.fotmob_match_payloads_current',
    'iceberg.bronze.fotmob_competition_seasons_current',
}
BLOCK_RE = re.compile(r'-- fotmob_match_key:begin\n.*?-- fotmob_match_key:end\n', re.DOTALL)
PLACEHOLDERS = ('fotmob_calendar_scope', 'fotmob_payload_scope')

# Season key of each saved card (verify23 records, bronze _current 24.10).
SEASON_KEYS = {
    (47, 4813374): '2025/2026', (47, 4813375): '2025/2026', (47, 4813571): '2025/2026',
    (47, 4813748): '2025/2026', (47, 5795363): '2026/2027', (47, 5795370): '2026/2027',
    (47, 5795438): '2026/2027', (47, 5795459): '2026/2027', (50, 4043971): '2024',
    (50, 4043980): '2024', (63, 5847127): '2026/2027', (77, 3370572): '2022',
    (8947, 4883482): '2025/2026', (8947, 4883771): '2025/2026', (8947, 5947993): '2026/2027',
    (8947, 5947998): '2026/2027', (8947, 5953439): '2026/2027', (8947, 5953825): '2026/2027',
    (9806, 4421978): '2024/2025', (9806, 4421988): '2024/2025', (9806, 4679444): '2024/2025',
    (9806, 4679447): '2024/2025', (9806, 5181793): '2026/2027',
}

CALENDAR_COLUMNS = {
    'competition_id': 'BIGINT', 'source_season_key': 'VARCHAR', 'scope_stage_id': 'VARCHAR',
    'match_id': 'BIGINT', 'utc_time': 'VARCHAR', 'timezone': 'VARCHAR', 'postponed': 'VARCHAR',
    'score_text': 'VARCHAR', 'status_reason_short': 'VARCHAR', 'status_reason_short_key': 'VARCHAR',
    'status_reason_long': 'VARCHAR', 'status_reason_long_key': 'VARCHAR',
    'home_team_id': 'BIGINT', 'home_team_name': 'VARCHAR', 'home_team_short_name': 'VARCHAR',
    'away_team_id': 'BIGINT', 'away_team_name': 'VARCHAR', 'away_team_short_name': 'VARCHAR',
    'home_score': 'BIGINT', 'away_score': 'BIGINT', 'finished': 'BOOLEAN', 'started': 'BOOLEAN',
    'cancelled': 'BOOLEAN', 'awarded': 'BOOLEAN', 'round_id': 'VARCHAR', 'round_name': 'VARCHAR',
    'stage_id': 'VARCHAR', 'group_name': 'VARCHAR', 'page_url': 'VARCHAR', 'source_path': 'VARCHAR',
}
PAYLOAD_COLUMNS = {
    'competition_id': 'VARCHAR', 'source_season_key': 'VARCHAR', 'match_id': 'BIGINT',
    'page_url': 'VARCHAR', 'utc_time': 'VARCHAR', 'home_team_id': 'BIGINT', 'away_team_id': 'BIGINT',
    **{c: 'VARCHAR' for c in (
        'available_sections_json', 'expected_sections_json', 'missing_expected_sections_json',
        'match_facts_json', 'stats_json', 'player_stats_json', 'lineup_json', 'shotmap_json',
        'h2h_json', 'payload_json', 'momentum_json',
    )},
}
LINEAGE_COLUMNS = {
    '_target_batch_id': 'VARCHAR', '_payload_sha256': 'VARCHAR', '_parser_version': 'VARCHAR',
    '_raw_uri': 'VARCHAR', '_source': 'VARCHAR', '_entity_type': 'VARCHAR',
    '_observed_at': 'TIMESTAMP', '_ingested_at': 'TIMESTAMP',
}
SEASON_COLUMNS = {
    'competition_id': 'VARCHAR', 'source_season_key': 'VARCHAR',
    '_observed_at': 'TIMESTAMP', '_ingested_at': 'TIMESTAMP',
}
DUCKDB_TYPES = {
    'bigint': 'BIGINT', 'integer': 'INTEGER', 'double': 'DOUBLE', 'boolean': 'BOOLEAN',
    'varchar': 'VARCHAR', 'decimal(38,8)': 'DECIMAL(38,8)', 'timestamp(6)': 'TIMESTAMP',
    'timestamp(6) with time zone': 'TIMESTAMP WITH TIME ZONE', 'array(varchar)': 'VARCHAR[]',
}
T0 = datetime(2026, 10, 1, 3, 0, 0)


# ---------------------------------------------------------------------------- helpers

def _load_audit():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        'audit_silver_charter_2017', ROOT / 'scripts' / 'audit_silver_charter.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


AUDIT = _load_audit()
SCHEMA = yaml.safe_load(SCHEMA_PATH.read_text(encoding='utf-8'))


def _sql_path(table: str) -> Path:
    return SILVER / f'{table}.sql.j2'


def _duckdb_sql(table: str) -> str:
    from utils.medallion_config import render_sql_template
    rendered = render_sql_template(
        _sql_path(table), fotmob_calendar_scope='TRUE', fotmob_payload_scope='TRUE')
    sql = sqlglot.transpile(rendered, read='trino', write='duckdb')[0]
    return sql.replace('iceberg.bronze.', 'bronze.')


def _load_cards() -> dict[tuple[int, int], dict]:
    cards = {}
    for path in sorted(CARDS_DIR.glob('*.json.gz')):
        comp, match = path.name[:-len('.json.gz')].split('_')
        with gzip.open(path, 'rt', encoding='utf-8') as fh:
            cards[(int(comp), int(match))] = json.load(fh)
    return cards


CARDS = _load_cards()


def calendar_row(comp: int, key: str, card: dict, *, observed: datetime = T0,
                 batch: str | None = None, **over) -> dict:
    general, header = card['general'], card['header']
    status = header.get('status') or {}
    reason = status.get('reason') or {}
    teams = header.get('teams') or [{}, {}]
    match_id = int(general['matchId'])
    row = {
        'competition_id': comp, 'source_season_key': key, 'scope_stage_id': None,
        'match_id': match_id, 'utc_time': status.get('utcTime') or general.get('matchTimeUTCDate'),
        'timezone': None, 'postponed': None, 'score_text': status.get('scoreStr'),
        'status_reason_short': reason.get('short'), 'status_reason_short_key': reason.get('shortKey'),
        'status_reason_long': reason.get('long'), 'status_reason_long_key': reason.get('longKey'),
        'home_team_id': general['homeTeam']['id'], 'home_team_name': general['homeTeam']['name'],
        'home_team_short_name': teams[0].get('name'),
        'away_team_id': general['awayTeam']['id'], 'away_team_name': general['awayTeam']['name'],
        'away_team_short_name': teams[1].get('name'),
        'home_score': teams[0].get('score'), 'away_score': teams[1].get('score'),
        'finished': status.get('finished'), 'started': status.get('started'),
        'cancelled': status.get('cancelled'), 'awarded': status.get('awarded'),
        'round_id': general.get('matchRound'), 'round_name': general.get('leagueRoundName'),
        'stage_id': None, 'group_name': None, 'page_url': f'/matches/x/{match_id}',
        'source_path': None,
        '_target_batch_id': batch or f'cal-{comp}-{key}-{match_id}', '_payload_sha256': None,
        '_parser_version': 'fotmob-native-v2-cal', '_raw_uri': None, '_source': 'fotmob',
        '_entity_type': 'match', '_observed_at': observed, '_ingested_at': observed + timedelta(minutes=5),
    }
    row.update(over)
    return row


def payload_row(comp, key: str, card: dict, *, observed: datetime = T0 + timedelta(hours=1),
                batch: str | None = None, **over) -> dict:
    general = card['general']
    match = {
        'competition_id': comp, 'source_season_key': key, 'match_id': int(general['matchId']),
        'page_url': f'/matches/x/{general["matchId"]}', 'utc_time': general.get('matchTimeUTCDate'),
        'home_team_id': general['homeTeam']['id'], 'away_team_id': general['awayTeam']['id'],
        'finished': general.get('finished'),
    }
    row, _ = FotMobIngestService._match_payload_row(match, card)
    row = normalize_rows([row])[0]
    row.update({
        '_target_batch_id': batch or f'card-{comp}-{key}-{general["matchId"]}',
        '_payload_sha256': None, '_parser_version': 'fotmob-native-v2', '_raw_uri': None,
        '_source': 'fotmob', '_entity_type': 'match', '_observed_at': observed,
        '_ingested_at': observed + timedelta(minutes=5),
    })
    row.update(over)
    return row


def season_row(comp, key: str) -> dict:
    return {'competition_id': str(comp), 'source_season_key': key, '_observed_at': T0, '_ingested_at': T0}


class Bronze:
    def __init__(self, calendar=(), payloads=(), seasons=()):
        self.calendar, self.payloads, self.seasons = list(calendar), list(payloads), list(seasons)
        self._con = None
        self._cache: dict[str, tuple[list[dict], list[tuple[str, str]]]] = {}

    def con(self):
        if self._con is None:
            con = duckdb.connect(':memory:')
            con.execute("SET TimeZone = 'UTC'")
            con.execute('CREATE SCHEMA bronze')
            for name, cols, rows in (
                ('fotmob_matches_current', {**CALENDAR_COLUMNS, **LINEAGE_COLUMNS}, self.calendar),
                ('fotmob_match_payloads_current', {**PAYLOAD_COLUMNS, **LINEAGE_COLUMNS}, self.payloads),
                ('fotmob_competition_seasons_current', SEASON_COLUMNS, self.seasons),
            ):
                ddl = ', '.join(f'"{c}" {t}' for c, t in cols.items())
                con.execute(f'CREATE TABLE bronze.{name} ({ddl})')
                if rows:
                    marks = ', '.join('?' for _ in cols)
                    con.executemany(f'INSERT INTO bronze.{name} VALUES ({marks})',
                                    [[r.get(c) for c in cols] for r in rows])
            self._con = con
        return self._con

    def run(self, table: str):
        if table not in self._cache:
            cur = self.con().execute(_duckdb_sql(table))
            names = [d[0] for d in cur.description]
            types = [(d[0], str(d[1])) for d in cur.description]
            self._cache[table] = ([dict(zip(names, r)) for r in cur.fetchall()], types)
        return self._cache[table]

    def rows(self, table: str) -> list[dict]:
        return self.run(table)[0]


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


def _ts(v):
    if not isinstance(v, str):
        return None
    try:
        return datetime.fromisoformat(v.replace('Z', '+00:00')).astimezone(timezone.utc)
    except ValueError:
        return None


def convert(value, dtype: str):
    dtype = dtype.lower()
    if dtype in ('bigint', 'integer'):
        return _int(value)
    if dtype == 'double':
        return _float(value)
    if dtype == 'boolean':
        return _bool(value)
    if dtype == 'decimal(38,8)':
        return None if _text(value) is None else Decimal(_text(value))
    if dtype == 'timestamp(6) with time zone':
        return _ts(value)
    if dtype == 'array(varchar)':
        return None if not isinstance(value, list) else [_text(x) for x in value]
    if dtype == 'varchar':
        return _text(value)
    raise AssertionError(dtype)


def _section(card_row, column):
    raw = card_row.get(column) if card_row else None
    return None if raw is None else json.loads(raw)


def resolve(source: str, ctx: dict):
    """Value of a yaml ``from`` (non-derived) in the row context ``ctx``."""
    view, _, rest = source.partition('.')
    if view == 'fotmob_matches_current':
        return (ctx.get('cal') or {}).get(rest)
    assert view == 'fotmob_match_payloads_current', source
    if ':' not in rest:
        if rest.endswith('_json'):
            return _section(ctx.get('card'), rest)
        return (ctx.get('card') or {}).get(rest)
    bindings = ctx.get('bind', {})
    best = max((p for p in bindings if rest == p or rest.startswith(p + '.')), key=len, default=None)
    if best is not None:
        return jpath(bindings[best], '$' + rest[len(best):])
    column, _, path = rest.partition(':')
    return jpath(_section(ctx.get('card'), column), path)


def oracle_base(bronze: Bronze) -> list[dict]:
    """Python mirror of the shared block: one entry per match with its key and chosen card."""
    def valid(v):
        return v is not None and v != ''

    cal = {}
    for r in bronze.calendar:
        if not all(valid(r.get(c)) for c in ('competition_id', 'match_id', 'source_season_key')):
            continue
        k = (r['competition_id'], r['source_season_key'], r['match_id'])
        if k not in cal or (r['_observed_at'], r['_ingested_at']) > (cal[k]['_observed_at'], cal[k]['_ingested_at']):
            cal[k] = r
    by_match: dict = {}
    for (comp, key, match), r in cal.items():
        by_match.setdefault((comp, match), {})[key] = r
    chosen = {}
    for m, rows in by_match.items():
        keys = set(rows)

        def rank(k):
            parent = re.match(r'^(.+?) - (.+)$', k)
            return (1 if parent and parent.group(1) in keys else 0, len(k), k)
        chosen[m] = rows[max(keys, key=rank)]
    cards = {}
    for r in bronze.payloads:
        comp = _int(r.get('competition_id'))
        if comp is None or r.get('match_id') is None:
            continue
        k = (comp, r['match_id'])
        order = (r['_observed_at'], r['_ingested_at'], r['_target_batch_id'])
        if k not in cards or order > cards[k][0]:
            cards[k] = (order, r)
    base = []
    for m in sorted(set(chosen) | set(cards)):
        c, card = chosen.get(m), (cards[m][1] if m in cards else None)
        key = c['source_season_key'] if c else card['source_season_key']
        if not valid(key):
            continue
        cal_view = dict(c) if c else {}
        # Decision #2017: calendar fields fall back to the card row's calendar copy.
        for col in ('home_team_id', 'away_team_id', 'page_url', 'utc_time'):
            if cal_view.get(col) is None and card is not None:
                cal_view[col] = card.get(col)
        lineage_src = card if card is not None else c
        base.append({
            'competition_id': m[0], 'match_id': m[1], 'source_season_key': key, 'cal': cal_view,
            'card': card, 'has_payload': card is not None,
            'lineage': {
                '_bronze_batch_id': lineage_src['_target_batch_id'],
                '_bronze_observed_at': lineage_src['_observed_at'],
                '_bronze_ingested_at': lineage_src['_ingested_at'],
                '_parser_version': lineage_src['_parser_version'],
            },
        })
    return base


def _spec(table):
    return SCHEMA['tables'][table]


def _row(table: str, b: dict, ctx: dict, derived: dict, issues: list | None = None) -> dict:
    """Build one expected row: yaml ``from`` for non-derived columns, ``derived`` for the rest."""
    spec = _spec(table)
    ctx = {'cal': b['cal'], 'card': b['card'], **ctx}
    out = {'competition_id': b['competition_id'], 'source_season_key': b['source_season_key'],
           'match_id': b['match_id']}
    for col in spec['columns']:
        name = col['name']
        if name in out:
            continue
        if name in derived:
            out[name] = derived[name]
            continue
        assert not str(col['from']).startswith('derived'), f'{table}.{name} needs a derived rule'
        value = convert(resolve(col['from'], ctx), col['type'])
        if 'id 0' in str(col.get('rule', '')) and value == 0:
            value = None
        out[name] = value
    for col in spec.get('raw_json', []):
        out[col['name']] = resolve(col['from'], ctx)
    out.update(b['lineage'])
    out['_conform_issues_json'] = issues or None
    return out


def _issue(column, raw, converted):
    raw = None if raw in (None, '') else raw
    return [{'column': column, 'raw': raw}] if raw is not None and converted is None else []


def _team_ids(b):
    return b['cal'].get('home_team_id'), b['cal'].get('away_team_id')


def _side_by_team(team_id, b):
    home, away = _team_ids(b)
    return 'home' if team_id is not None and team_id == home else (
        'away' if team_id is not None and team_id == away else None)


def _num_raw(raw):
    m = re.search(r'-?[0-9]+(\.[0-9]+)?', raw or '')
    return float(m.group(0)) if m else None


def _pct_raw(raw):
    m = re.search(r'\((-?[0-9]+(\.[0-9]+)?)\s*%\)', raw or '')
    return float(m.group(1)) if m else None


def _lineup_entries(card_row):
    lineup = _section(card_row, 'lineup_json')
    for side in ('home', 'away'):
        team = jpath(lineup, f'$.{side}Team')
        for role, arr in (('starter', 'starters'), ('sub', 'subs')):
            players = jpath(team, f'$.{arr}')
            for idx, player in enumerate(players if isinstance(players, list) else [], start=1):
                yield side, role, idx, team, player


def oracle(table: str, bronze: Bronze) -> list[dict]:
    out = []
    for b in oracle_base(bronze):
        card = b['card']
        if table == 'fotmob_matches':
            parent = re.match(r'^(.+?) - (.+)$', b['source_season_key'])
            parent_key = parent.group(1) if parent else None
            known = {(str(s['competition_id']), s['source_season_key']) for s in bronze.seasons}
            general = jpath(_section(card, 'payload_json'), '$.general')
            info = jpath(_section(card, 'match_facts_json'), '$.infoBox')
            kickoff_raw = _text(jpath(general, '$.matchTimeUTCDate'))
            ref_raw = _text(jpath(info, '$.Referee.id'))
            cal = b['cal']
            issues = (_issue('kickoff_utc', kickoff_raw, _ts(kickoff_raw))
                      + _issue('calendar_kickoff_utc', cal.get('utc_time'), _ts(cal.get('utc_time')))
                      + _issue('postponed', cal.get('postponed'), _bool(cal.get('postponed')))
                      + _issue('referee_id', ref_raw, _int(ref_raw)))
            out.append(_row(table, b, {}, {
                'parent_season_key': parent_key if (str(b['competition_id']), parent_key) in known else None,
                'calendar_kickoff_utc': _ts(cal.get('utc_time')),
                'has_payload': b['has_payload'],
            }, issues))
        elif table == 'fotmob_match_teams':
            payload = _section(card, 'payload_json')
            lineup = _section(card, 'lineup_json')
            for i, side in enumerate(('home', 'away')):
                bind = {
                    'payload_json:$.general.homeTeam': jpath(payload, f'$.general.{side}Team'),
                    'payload_json:$.header.teams[*]': jpath(payload, f'$.header.teams[{i}]'),
                    'lineup_json:$.homeTeam': jpath(lineup, f'$.{side}Team'),
                    'payload_json:$.content.attackingZones.home': jpath(payload, f'$.content.attackingZones.{side}'),
                }
                gt = bind['payload_json:$.general.homeTeam']
                team_id = _int(jpath(gt, '$.id'))
                team_name = _text(jpath(gt, '$.name'))
                out.append(_row(table, b, {'bind': bind}, {
                    'side': side,
                    # Decision #2017: COALESCE(card general.<side>Team, calendar).
                    'team_id': team_id if team_id is not None else _team_ids(b)[i],
                    'team_name': team_name if team_name is not None else b['cal'].get(f'{side}_team_name'),
                    'total_starter_market_value_currency': None,
                }))
        elif card is None:
            continue
        elif table == 'fotmob_match_team_stats':
            periods = jpath(_section(card, 'stats_json'), '$.Periods')
            for period, pobj in (periods.items() if isinstance(periods, dict) else []):
                for group in jpath(pobj, '$.stats') or []:
                    for stat in jpath(group, '$.stats') or []:
                        if jpath(stat, '$.type') == 'title':
                            continue
                        values = jpath(stat, '$.stats')
                        for i, value in enumerate((values if isinstance(values, list) else [])[:2]):
                            side = ('home', 'away')[i]
                            raw = _text(value)
                            num, pct = _num_raw(raw), _pct_raw(raw)
                            issues = _issue('stat_value', raw, num) + (
                                [{'column': 'stat_value_pct', 'raw': raw}] if raw and '%' in raw and pct is None else [])
                            out.append(_row(table, b, {'bind': {
                                'stats_json:$.Periods.All.stats[*]': group,
                                'stats_json:$.Periods.All.stats[*].stats[*]': stat,
                                'stats_json:$.Periods.All.stats[*].stats[*].stats[*]': value,
                            }}, {
                                'period': period, 'side': side, 'team_id': _team_ids(b)[i],
                                'stat_value': num, 'stat_value_pct': pct,
                            }, issues))
        elif table == 'fotmob_match_lineups':
            for side, role, idx, team, player in _lineup_entries(card):
                subs = jpath(player, '$.performance.substitutionEvents') or []

                def sub(kind):
                    hit = [e for e in subs if isinstance(e, dict) and e.get('type') == kind]
                    return _int(hit[0].get('time')) if hit else None
                shirt_raw = _text(jpath(player, '$.shirtNumber'))
                fantasy_raw = _text(jpath(player, '$.performance.fantasyScore'))
                out.append(_row(table, b, {'bind': {
                    'lineup_json:$.homeTeam': team, 'lineup_json:$.homeTeam.starters[*]': player,
                }}, {
                    'local_player_key': f'{side}:{role}:{idx}', 'side': side, 'lineup_role': role,
                    'lineup_index': idx, 'market_value_currency': None,
                    'sub_in_minute': sub('subIn'), 'sub_out_minute': sub('subOut'),
                }, _issue('shirt_number', shirt_raw, _int(shirt_raw))
                   + _issue('fantasy_score', fantasy_raw, _float(fantasy_raw))))
        elif table == 'fotmob_match_player_stats':
            by_id, by_opta = {}, {}
            for side, role, idx, _team, player in _lineup_entries(card):
                key = f'{side}:{role}:{idx}'
                pid = _int(player.get('id'))
                if pid:
                    by_id.setdefault(pid, []).append(key)
                if player.get('optaId') is not None:
                    by_opta.setdefault(player['optaId'], []).append(key)
            stats = _section(card, 'player_stats_json')
            for pkey, player in (stats.items() if isinstance(stats, dict) else []):
                pid = _int(player.get('id')) or None
                opta = _text(player.get('optaId'))
                hits = by_id.get(pid, []) if pid else by_opta.get(opta, []) if opta else []
                local = min(hits) if hits else f'stats:{pkey}'
                for group in jpath(player, '$.stats') or []:
                    if not jpath(group, '$.key'):
                        continue
                    for title, stat in (jpath(group, '$.stats') or {}).items():
                        out.append(_row(table, b, {'bind': {
                            'player_stats_json:$.*': player,
                            'player_stats_json:$.*.stats[*]': group,
                            'player_stats_json:$.*.stats[*].stats.*': stat,
                        }}, {
                            'local_player_key': local, 'stat_title': title,
                            'side': _side_by_team(_int(player.get('teamId')), b),
                        }))
        elif table == 'fotmob_match_events':
            facts = _section(card, 'match_facts_json')
            for section, path in (('events', '$.events.events'),
                                  ('penalty_shootout', '$.events.penaltyShootoutEvents')):
                for idx, event in enumerate(jpath(facts, path) or [], start=1):
                    is_home = _bool(event.get('isHome'))
                    side = None if is_home is None else ('home' if is_home else 'away')
                    out.append(_row(table, b, {'bind': {'match_facts_json:$.events.events[*]': event}}, {
                        'event_section': section, 'event_index': idx, 'side': side,
                        'team_id': None if side is None else _team_ids(b)[0 if is_home else 1],
                    }))
        elif table == 'fotmob_shots':
            for shot in jpath(_section(card, 'shotmap_json'), '$.shots') or []:
                if _int(shot.get('id')) is None:
                    continue
                out.append(_row(table, b, {'bind': {'shotmap_json:$.shots[*]': shot}}, {
                    'side': _side_by_team(_int(shot.get('teamId')), b),
                    'is_penalty_shootout': shot.get('period') == 'PenaltyShootout',
                }))
        elif table == 'fotmob_match_momentum':
            momentum = _section(card, 'momentum_json')
            main = jpath(momentum, '$.main')
            for point in jpath(main, '$.data') or []:
                if _float(point.get('minute')) is None:
                    continue
                out.append(_row(table, b, {'bind': {
                    'momentum_json:$.main': main, 'momentum_json:$.main.data[*]': point,
                }}, {}))
        else:
            raise AssertionError(table)
    key = _spec(table)['natural_key']
    return [r for r in out if all(r[c] is not None and r[c] != '' for c in key)]


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
    if dtype == 'varchar' and got is not None and want is not None and got != want:
        return _float(got) is not None and _close(_float(got), _float(want))
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
        f'{table}: keys differ; only SQL {sorted(set(got_by) - set(want_by))[:5]}, '
        f'only oracle {sorted(set(want_by) - set(got_by))[:5]}')
    expected = AUDIT.core_expected_columns(SCHEMA, table)
    mismatches = []
    for k, w in want_by.items():
        g = got_by[k]
        for column, dtype in expected.items():
            if not _same(column, dtype, g[column], w[column]):
                mismatches.append((k, column, g[column], w[column]))
    assert not mismatches, f'{table}: {len(mismatches)} value mismatches, e.g. {mismatches[:3]}'
    return got


# ---------------------------------------------------------------------------- fixtures

@pytest.fixture(scope='module')
def cards_bronze() -> Bronze:
    calendar, payloads = [], []
    for i, ((comp, match), card) in enumerate(sorted(CARDS.items())):
        key = SEASON_KEYS[(comp, match)]
        calendar.append(calendar_row(comp, key, card))
        payloads.append(payload_row(comp, key, card, observed=T0 + timedelta(hours=1, minutes=i)))
    seasons = sorted({(comp, key) for (comp, _), key in SEASON_KEYS.items()})
    return Bronze(calendar, payloads, [season_row(comp, key) for comp, key in seasons])


def _by(rows, **flt):
    return [r for r in rows if all(r[k] == v for k, v in flt.items())]


# ---------------------------------------------------------------------------- static

def _text_of(table):
    return _sql_path(table).read_text(encoding='utf-8')


def test_shared_block_identical_in_all_files():
    blocks = {t: BLOCK_RE.findall(_text_of(t)) for t in TABLES}
    assert all(len(b) == 1 for b in blocks.values()), {t: len(b) for t, b in blocks.items()}
    assert len({b[0] for b in blocks.values()}) == 1


@pytest.mark.parametrize('table', TABLES)
def test_pure_select_sources_and_placeholders(table):
    text = _text_of(table)
    code = AUDIT.strip_sql(text)
    assert not re.search(r'\b(CREATE|INSERT|DELETE|MERGE|DROP|ALTER)\b', code, re.IGNORECASE)
    assert not re.search(r'WITH\s*\(\s*partitioning', code, re.IGNORECASE)
    refs = set(re.findall(r'iceberg\.\w+\.\w+', code))
    assert refs and refs <= BRONZE_SOURCES, refs
    for name in PLACEHOLDERS:
        assert re.search(r'^[ \t]*\{\{ ' + name + r' \}\}[ \t]*$', text, re.MULTILINE), name
    assert set(re.findall(r'\{\{\s*(\w+)\s*\}\}', code)) == set(PLACEHOLDERS)


def test_audit_layer_a_clean():
    cores = AUDIT.load_core_schemas()
    for table in TABLES:
        findings = AUDIT.audit_file(_sql_path(table), cores)
        assert not findings, (table, findings)


@pytest.mark.parametrize('table', TABLES)
def test_columns_order_and_types(cards_bronze, table):
    _, types = cards_bronze.run(table)
    expected = AUDIT.core_expected_columns(SCHEMA, table)
    assert [n for n, _ in types] == list(expected)
    assert {n: t for n, t in types} == {n: DUCKDB_TYPES[t] for n, t in expected.items()}


# ---------------------------------------------------------------------------- 23 cards

@pytest.mark.parametrize('table', TABLES)
def test_cards_match_oracle(cards_bronze, table):
    assert_matches_oracle(cards_bronze, table)


EXPECTED_ROWS = {
    'fotmob_matches': 23, 'fotmob_match_teams': 46, 'fotmob_match_lineups': 888,
    'fotmob_match_events': 396, 'fotmob_shots': 480, 'fotmob_match_momentum': 1693,
    'fotmob_match_team_stats': 5052, 'fotmob_match_player_stats': 18021,
}


@pytest.mark.parametrize('table', TABLES)
def test_cards_row_counts(cards_bronze, table):
    assert len(cards_bronze.rows(table)) == EXPECTED_ROWS[table]


@pytest.mark.parametrize('match_id,shootout,penalties', [
    (4043980, 9, (3, 5)), (3370572, 8, (4, 2)), (4679444, 14, (5, 4)),
])
def test_penalty_shootouts_kept_out_of_score(cards_bronze, match_id, shootout, penalties):
    events = _by(cards_bronze.rows('fotmob_match_events'), match_id=match_id, event_section='penalty_shootout')
    shots = _by(cards_bronze.rows('fotmob_shots'), match_id=match_id, is_penalty_shootout=True)
    match, = _by(cards_bronze.rows('fotmob_matches'), match_id=match_id)
    assert len(events) == shootout and len(shots) == shootout
    assert all(s['period'] == 'PenaltyShootout' for s in shots)
    assert (match['home_penalty_score'], match['away_penalty_score']) == penalties
    header = CARDS[(match['competition_id'], match_id)]['header']['teams']
    assert (match['home_score'], match['away_score']) == (header[0]['score'], header[1]['score'])
    teams = _by(cards_bronze.rows('fotmob_match_teams'), match_id=match_id)
    assert sorted((t['side'], t['score']) for t in teams) == sorted(
        [('home', header[0]['score']), ('away', header[1]['score'])])


def test_player_id_zero_kept_with_opta_and_local_key(cards_bronze):
    rows = _by(cards_bronze.rows('fotmob_match_lineups'), competition_id=8947)
    zero = [r for r in rows if r['player_id'] is None]
    per_match = {}
    for r in zero:
        per_match[r['match_id']] = per_match.get(r['match_id'], 0) + 1
    assert per_match == {4883771: 33, 5947993: 35, 5947998: 35, 5953825: 34}
    assert len(zero) == 137
    assert all(r['opta_id'] and r['local_player_key'] for r in zero)


def test_8947_cards_have_no_stat_children(cards_bronze):
    for table in ('fotmob_match_team_stats', 'fotmob_match_player_stats', 'fotmob_match_momentum', 'fotmob_shots'):
        assert not _by(cards_bronze.rows(table), competition_id=8947), table
    for match_id in (4883482, 5953439):
        match, = _by(cards_bronze.rows('fotmob_matches'), match_id=match_id)
        assert match['has_payload'] is True and match['has_lineup'] is False
        assert not _by(cards_bronze.rows('fotmob_match_lineups'), match_id=match_id)
        assert len(_by(cards_bronze.rows('fotmob_match_teams'), match_id=match_id)) == 2


# ---------------------------------------------------------------------------- scenarios

RICH = (9806, 4679444)
OTHER = (47, 4813374)


def _assert_all_tables(bronze: Bronze):
    for table in TABLES:
        assert_matches_oracle(bronze, table)


def _keys_of(bronze: Bronze, match_id: int) -> dict[str, set]:
    return {t: {r['source_season_key'] for r in bronze.rows(t) if r['match_id'] == match_id} for t in TABLES}


def test_split_season_card_under_parent_goes_to_child_key():
    card = CARDS[RICH]
    bronze = Bronze(
        calendar=[calendar_row(112, '2026', card), calendar_row(112, '2026 - Apertura', card)],
        payloads=[payload_row(112, '2026', card)],
        seasons=[season_row(112, k) for k in ('2026', '2026 - Apertura', '2026 - Clausura')],
    )
    _assert_all_tables(bronze)
    match, = bronze.rows('fotmob_matches')
    assert match['source_season_key'] == '2026 - Apertura' and match['parent_season_key'] == '2026'
    keys = _keys_of(bronze, RICH[1])
    assert all(v == {'2026 - Apertura'} for v in keys.values()), keys
    assert len(bronze.rows('fotmob_shots')) == len(card['content']['shotmap']['shots'])


def test_two_card_versions_take_latest_without_mixing():
    card = CARDS[RICH]
    old = copy.deepcopy(card)
    old['content']['shotmap']['shots'] = old['content']['shotmap']['shots'][:1]
    old['content']['lineup']['homeTeam']['starters'][0]['shirtNumber'] = '99'
    bronze = Bronze(
        calendar=[calendar_row(112, '2026', card), calendar_row(112, '2026 - Apertura', card)],
        payloads=[
            payload_row(112, '2026 - Apertura', old, observed=T0 + timedelta(hours=1), batch='old'),
            payload_row(112, '2026', card, observed=T0 + timedelta(hours=2), batch='new'),
        ],
        seasons=[season_row(112, '2026')],
    )
    _assert_all_tables(bronze)
    for table in TABLES:
        assert {r['_bronze_batch_id'] for r in bronze.rows(table)} == {'new'}, table
    assert len(bronze.rows('fotmob_shots')) == len(card['content']['shotmap']['shots'])
    first = _by(bronze.rows('fotmob_match_lineups'), local_player_key='home:starter:1')[0]
    assert first['shirt_number'] == int(card['content']['lineup']['homeTeam']['starters'][0]['shirtNumber'])


def test_unrelated_keys_take_longest_then_greatest():
    a, b = CARDS[RICH], CARDS[OTHER]
    bronze = Bronze(
        calendar=[calendar_row(5, 'A1', a), calendar_row(5, 'B22', a),
                  calendar_row(5, 'AB', b), calendar_row(5, 'AC', b)],
        payloads=[payload_row(5, 'A1', a), payload_row(5, 'AB', b)],
    )
    _assert_all_tables(bronze)
    keys = {r['match_id']: r['source_season_key'] for r in bronze.rows('fotmob_matches')}
    assert keys == {RICH[1]: 'B22', OTHER[1]: 'AC'}
    assert _keys_of(bronze, RICH[1])['fotmob_shots'] == {'B22'}


def test_card_without_calendar_and_calendar_without_card():
    a, b = CARDS[RICH], CARDS[OTHER]
    bronze = Bronze(calendar=[calendar_row(9806, '2024/2025', b)],
                    payloads=[payload_row(9806, '2024/2025', a)])
    _assert_all_tables(bronze)
    by_id = {r['match_id']: r for r in bronze.rows('fotmob_matches')}
    card_only, cal_only = by_id[RICH[1]], by_id[OTHER[1]]
    assert card_only['has_payload'] is True and card_only['source_season_key'] == '2024/2025'
    assert card_only['home_team_name'] is None and card_only['home_team_id'] == a['general']['homeTeam']['id']
    assert card_only['_bronze_batch_id'].startswith('card-')
    assert cal_only['has_payload'] is False and cal_only['has_lineup'] is None
    assert cal_only['_bronze_batch_id'].startswith('cal-')
    teams = _by(bronze.rows('fotmob_match_teams'), match_id=OTHER[1])
    assert sorted((t['side'], t['team_id'], t['team_name']) for t in teams) == sorted([
        ('home', b['general']['homeTeam']['id'], b['general']['homeTeam']['name']),
        ('away', b['general']['awayTeam']['id'], b['general']['awayTeam']['name'])])
    for table in TABLES[2:]:
        assert not _by(bronze.rows(table), match_id=OTHER[1]), table
        assert _by(bronze.rows(table), match_id=RICH[1]), table


def test_bad_optional_value_is_null_with_conform_issue():
    card = copy.deepcopy(CARDS[RICH])
    card['content']['lineup']['homeTeam']['starters'][0]['shirtNumber'] = '12a'
    card['content']['matchFacts']['infoBox'].setdefault('Referee', {})['id'] = 'ref-x'
    bronze = Bronze(calendar=[calendar_row(9806, '2024/2025', card, postponed='maybe')],
                    payloads=[payload_row(9806, '2024/2025', card)])
    _assert_all_tables(bronze)
    row, = _by(bronze.rows('fotmob_match_lineups'), local_player_key='home:starter:1')
    assert row['shirt_number'] is None
    assert json.loads(row['_conform_issues_json']) == [
        {'column': 'shirt_number', 'raw': '12a', 'reason': 'not an integer'}]
    match, = bronze.rows('fotmob_matches')
    assert match['referee_id'] is None and match['postponed'] is None
    assert {i['column'] for i in json.loads(match['_conform_issues_json'])} == {'referee_id', 'postponed'}
    clean = [r for r in bronze.rows('fotmob_match_lineups') if r['local_player_key'] != 'home:starter:1']
    assert all(r['_conform_issues_json'] is None for r in clean)


def test_bad_keys_drop_only_their_rows():
    card = copy.deepcopy(CARDS[RICH])
    shots = card['content']['shotmap']['shots']
    shots[0]['id'] = None
    shots[1]['id'] = 'abc'
    group = card['content']['stats']['Periods']['All']['stats'][0]
    group['key'] = None
    other = CARDS[OTHER]
    bronze = Bronze(
        calendar=[calendar_row(9806, '2024/2025', card), calendar_row(47, '2025/2026', other),
                  calendar_row(47, '', other, match_id=1), calendar_row(47, '2025/2026', other, match_id=None)],
        payloads=[payload_row(9806, '2024/2025', card), payload_row('x47', '2025/2026', other)],
    )
    _assert_all_tables(bronze)
    assert len(bronze.rows('fotmob_shots')) == len(shots) - 2
    assert {r['match_id'] for r in bronze.rows('fotmob_matches')} == {RICH[1], OTHER[1]}
    other_match, = _by(bronze.rows('fotmob_matches'), match_id=OTHER[1])
    assert other_match['has_payload'] is False
    group_rows = [r for r in bronze.rows('fotmob_match_team_stats') if r['period'] == 'All']
    assert all(r['stat_group'] for r in group_rows)
    full = Bronze(calendar=[calendar_row(9806, '2024/2025', CARDS[RICH])],
                  payloads=[payload_row(9806, '2024/2025', CARDS[RICH])])
    dropped = 2 * sum(1 for s in group['stats'] if s.get('type') != 'title')
    assert len(bronze.rows('fotmob_match_team_stats')) == len(full.rows('fotmob_match_team_stats')) - dropped
