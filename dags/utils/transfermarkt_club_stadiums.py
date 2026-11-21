"""Write paired tmapi club observations and their verified Iceberg receipt."""

from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from types import UnionType
from typing import Union, get_args, get_origin, get_type_hints

import pandas as pd

from dags.utils import transfermarkt_current_write as current
from dags.utils.transfermarkt_approval import load_standing_policy
from scrapers.transfermarkt import club_stadiums as parser
from scrapers.transfermarkt import scraper as tm
from scrapers.transfermarkt.models import FetchStatus
from scrapers.transfermarkt.writer import writer_deadline, writer_lock


CLUB_PROFILE_TABLE = 'iceberg.bronze.transfermarkt_club_profile_observations'
STADIUM_TABLE = 'iceberg.bronze.transfermarkt_stadium_observations'
RELATION_TABLE = 'iceberg.bronze.transfermarkt_club_stadium_relations'
RECEIPTS_TABLE = 'iceberg.ops.transfermarkt_club_stadium_receipts_v1'
KEYS = {
    CLUB_PROFILE_TABLE: ('club_id', 'observed_at'),
    STADIUM_TABLE: ('stadium_observation_key', 'club_id', 'observed_at'),
    RELATION_TABLE: ('club_id', 'stadium_observation_key', 'observed_at'),
    RECEIPTS_TABLE: ('receipt_id',),
}
ENTITY = {
    CLUB_PROFILE_TABLE: 'club_profile',
    STADIUM_TABLE: 'stadium',
    RELATION_TABLE: 'club_stadium_relation',
}
TIME = 'timestamp(6)'
TEXT = 'varchar'
METADATA = {'_source': TEXT, '_entity_type': TEXT, '_batch_id': TEXT, '_ingested_at': TIME}
CONTEXT = {
    'parser_revision': 'bigint', 'schema_revision': 'bigint', 'cycle_id': TEXT,
    'parent_cycle_id': TEXT, 'scope_id': TEXT, 'source_competition_id': TEXT, 'source_edition_id': TEXT,
    'scope_json': TEXT, 'run_id': TEXT, 'universe_version': TEXT,
}
PAIR = {
    'profile_raw_capture_id': TEXT, 'profile_raw_body_hash': TEXT,
    'profile_observed_at': TIME, 'stadium_raw_capture_id': TEXT,
    'stadium_raw_body_hash': TEXT, 'stadium_observed_at': TIME,
}
LINEAGE = {
    'raw_capture_id': TEXT, 'raw_body_hash': TEXT, 'source_url': TEXT,
    'captured_at': TIME, 'observed_at': TIME, 'extracted_at': TIME,
    **CONTEXT, **PAIR,
}
PROFILE_DATA = {
    'club_id': TEXT, 'url': TEXT, 'name': TEXT, 'short_name': TEXT,
    'abbreviation': TEXT, 'club_code': TEXT, 'official_name': TEXT, 'image': TEXT,
    'address_line_1': TEXT, 'address_line_2': TEXT, 'address_line_3': TEXT,
    'country_id': TEXT, 'colors': TEXT, 'is_national_team': 'boolean',
}


def _donor_schema(model):
    # Preserve the offline contract's declared column order and optional scalar types.
    hints = get_type_hints(model)
    schema = {}
    for field in fields(model):
        hint = hints[field.name]
        types = get_args(hint) if get_origin(hint) in (Union, UnionType) else (hint,)
        schema[field.name] = next((sql for kind, sql in (
            (bool, 'boolean'), (int, 'bigint'), (float, 'double'), (datetime, TIME),
        ) if kind in types), TEXT)
    return schema


VENUE_KEYS = {'club_id': TEXT, 'stadium_observation_key': TEXT, 'stadium_id': TEXT}
SCHEMAS = {
    CLUB_PROFILE_TABLE: {**PROFILE_DATA, 'canonical_json': TEXT, **LINEAGE, **METADATA},
    STADIUM_TABLE: {**_donor_schema(parser.Stadium), **VENUE_KEYS, 'status': TEXT,
                    'canonical_json': TEXT, **LINEAGE, **METADATA},
    RELATION_TABLE: {**_donor_schema(parser.ClubStadiumRelation), **VENUE_KEYS, 'status': TEXT,
                     'valid_from': TIME, 'valid_to': TIME, 'canonical_json': TEXT,
                     **LINEAGE, **METADATA},
    RECEIPTS_TABLE: {
        'receipt_id': TEXT, 'club_id': TEXT, **CONTEXT, **PAIR,
        'profile_source_url': TEXT, 'stadium_source_url': TEXT,
        'profile_status_code': 'bigint', 'stadium_status_code': 'bigint',
        'stadium_status': TEXT, 'empty_proof_json': TEXT, 'key_maps_json': TEXT,
        'expected_profile_count': 'bigint', 'expected_stadium_count': 'bigint',
        'expected_relation_count': 'bigint', 'expected_present_count': 'bigint',
        'expected_empty_count': 'bigint', 'profile_physical_hash': TEXT,
        'stadium_physical_hash': TEXT, 'relation_physical_hash': TEXT,
        'plan_sha256': TEXT, 'batch_id': TEXT, 'committed_at': TIME, 'receipt_sha256': TEXT,
    },
}


def _json(value):
    def encode(item):
        if isinstance(item, datetime):
            return _clock(item).isoformat(timespec='microseconds') + 'Z'
        raise TypeError(f'unsupported canonical value {type(item).__name__}')

    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
                      allow_nan=False, default=encode)


def _digest(value):
    return hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def _clock(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if not isinstance(value, datetime) or pd.isna(value):
        raise ValueError('missing or invalid observation timestamp')
    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _field(obj, key):
    return obj.get(key) if isinstance(obj, Mapping) else getattr(obj, key, None)


def _identifier(value, name):
    if isinstance(value, bool) or not isinstance(value, (str, int)) or not str(value).strip():
        raise ValueError(f'missing or invalid {name}')
    return str(value)


def validate_club_stadium_write_policy(policy):
    allowed = _field(policy, 'allowed_write_tables')
    if not isinstance(allowed, (list, tuple, set, frozenset)):
        raise current.CurrentWriteError('missing allowed_write_tables in Transfermarkt standing policy')
    missing = set(KEYS).difference(allowed)
    if missing:
        raise current.CurrentWriteError(
            'standing policy does not authorize club stadium tables: ' + ', '.join(sorted(missing)))


def _authorize(preflight):
    path = os.environ.get(
        'TM_STANDING_POLICY_PATH',
        '/opt/airflow/dags/configs/transfermarkt/standing_approval_policy.json',
    )
    policy = load_standing_policy(Path(path))
    validate_club_stadium_write_policy(policy)
    current._authorize(preflight)


@dataclass(frozen=True)
class _Capture:
    capture_id: str
    body_hash: str
    url: str
    observed_at: datetime
    status_code: int
    payload: dict | None
    cycle_id: str


@dataclass(frozen=True)
class _EmptyProof:
    profile_raw_capture_id: str
    profile_status_code: int
    stadium_raw_capture_id: str
    stadium_status_code: int


def _read_capture(store, outcome, url, scope_id, *, allow_404):
    status = outcome.status_code
    empty = allow_404 and status == 404
    if type(status) is not int or (not empty and (outcome.status != FetchStatus.OK or status != 200)):
        raise ValueError('expected a successful HTTP 200 raw capture')
    capture_id = _identifier(outcome.raw_capture_id, 'raw_capture_id')
    body_hash = _identifier(outcome.raw_body_hash, 'raw_body_hash')
    observed = _clock(outcome.raw_fetched_at)
    raw = store.load_capture(capture_id)
    if isinstance(raw, tuple) and len(raw) == 2:
        manifest, body = raw
        if isinstance(manifest, bytes):
            body, manifest = manifest, body
    else:
        manifest = _field(raw, 'manifest')
        if manifest is None:
            manifest = raw
        body = _field(raw, 'body')
    expected = {'capture_id': capture_id, 'content_hash': body_hash, 'url': url,
                'scope_id': scope_id, 'status_code': status}
    for key, value in expected.items():
        if _field(manifest, key) != value:
            raise ValueError(f'raw manifest {key} mismatch')
    capture_cycle = _identifier(_field(manifest, 'cycle_id'), 'raw cycle_id')
    if _clock(_field(manifest, 'fetched_at')) != observed:
        raise ValueError('raw manifest fetched_at mismatch')
    if not isinstance(body, bytes) or hashlib.sha256(body).hexdigest() != body_hash:
        raise ValueError('raw capture body hash mismatch')
    payload = None
    if not empty:
        payload = json.loads(body.decode('utf-8'))
        value = outcome.value
        if isinstance(value, (str, bytes)):
            value = json.loads(value)
        if not isinstance(payload, dict) or _json(payload) != _json(value):
            raise ValueError('decoded outcome differs from raw capture body')
    return _Capture(capture_id, body_hash, url, observed, status, payload, capture_cycle)


def _scope_identifier(scope, *names):
    for name in names:
        if scope.get(name) is not None:
            return _identifier(scope[name], name)
    raise ValueError('scope lacks ' + names[0])


def _donor_row(values):
    return {key: _json(value) if isinstance(value, (tuple, list, dict)) else value
            for key, value in values.items()}


def _make_plan(club, profile, stadium, country_names, scope, cycle_id, run_id, universe):
    context = {
        'parser_revision': parser.PARSER_REVISION, 'schema_revision': parser.SCHEMA_REVISION,
        'cycle_id': profile.cycle_id, 'parent_cycle_id': _identifier(cycle_id, 'cycle_id'),
        'scope_id': scope['scope_id'],
        'source_competition_id': _scope_identifier(
            scope, 'source_competition_id', 'competition_id', 'competition_code',
            'source_competition_code', 'source_competition', 'competition'),
        'source_edition_id': _scope_identifier(
            scope, 'source_edition_id', 'edition_id', 'source_edition', 'edition',
            'source_season_id', 'season_id', 'season'),
        'scope_json': _json(scope), 'run_id': _identifier(run_id, 'run_id'),
        'universe_version': _identifier(universe, 'universe_version'),
    }
    pair = {
        'profile_raw_capture_id': profile.capture_id, 'profile_raw_body_hash': profile.body_hash,
        'profile_observed_at': profile.observed_at, 'stadium_raw_capture_id': stadium.capture_id,
        'stadium_raw_body_hash': stadium.body_hash, 'stadium_observed_at': stadium.observed_at,
    }
    if profile.capture_id == stadium.capture_id:
        raise ValueError('profile and stadium require distinct raw captures')
    batch = _digest([parser.PARSER_REVISION, parser.SCHEMA_REVISION, club,
                     profile.capture_id, profile.body_hash, stadium.capture_id, stadium.body_hash])
    extracted = max(profile.observed_at, stadium.observed_at)

    def row(capture, values):
        return {**_donor_row(values), 'canonical_json': _json(values),
                'raw_capture_id': capture.capture_id, 'raw_body_hash': capture.body_hash,
                'source_url': capture.url, 'captured_at': capture.observed_at,
                'observed_at': capture.observed_at, 'extracted_at': extracted, **context, **pair}

    parsed = parser.parse_club_profile(profile.payload, club_id=club, country_names=country_names,
                                       observed_at=profile.observed_at.isoformat() + 'Z')
    rows = {CLUB_PROFILE_TABLE: [row(profile, parsed.as_dict())], STADIUM_TABLE: [],
            RELATION_TABLE: []}
    proof = None
    if stadium.status_code == 404:
        proof = _EmptyProof(profile.capture_id, profile.status_code, stadium.capture_id, 404)
    else:
        result = parser.parse_club_stadium(stadium.payload, club_id=club,
                                           observed_at=stadium.observed_at.isoformat() + 'Z',
                                           country_names=country_names)
        venue = asdict(result.stadium)
        relation = asdict(result.relation)
        for donor in (venue, relation):
            if donor.get('club_id') is not None and str(donor['club_id']) != club:
                raise ValueError('parsed stadium club differs from requested club')
        source_ids = {_identifier(value, 'stadium_id') for value in (
            venue.get('stadium_id'), relation.get('stadium_id'), venue.get('source_stadium_id'),
            venue.get('id'), venue.get('source_id'),
        ) if value is not None}
        if len(source_ids) > 1:
            raise ValueError('parsed stadium and relation have conflicting source IDs')
        stadium_id = next(iter(source_ids), None)
        key = f'stadium:{stadium_id}' if stadium_id is not None else f'club:{club}:unidentified'
        identification = {'club_id': club, 'stadium_id': stadium_id,
                          'stadium_observation_key': key, 'status': 'observed_present'}
        rows[STADIUM_TABLE].append(row(stadium, {**venue, **identification}))
        rows[RELATION_TABLE].append(row(stadium, {**relation, **identification,
                                                 'valid_from': None, 'valid_to': None}))
    frames = {}
    for table, items in rows.items():
        columns = [column for column in SCHEMAS[table] if column not in METADATA]
        if items:
            frame = tm._with_metadata(items, columns, entity_type=ENTITY[table], batch_id=batch,
                                      ingested_at=extracted)
            frames[table] = frame.loc[:, list(SCHEMAS[table])]
        else:
            frames[table] = pd.DataFrame(columns=list(SCHEMAS[table]))
    return frames, batch, context, pair, proof


def _records(frame, schema):
    """Normalize Trino/pandas scalar representations before hashing or comparison."""
    records = []
    for source in frame.loc[:, list(schema)].to_dict('records'):
        record = {}
        for key, kind in schema.items():
            value = source[key]
            if pd.isna(value):
                value = None
            elif kind == TIME:
                value = _clock(value)
            elif hasattr(value, 'item'):
                value = value.item()
            if value is not None and kind == 'boolean':
                if value not in (False, True):
                    raise ValueError(f'invalid physical boolean in {key}')
                value = bool(value)
            if value is not None and kind == 'bigint':
                if isinstance(value, bool) or int(value) != value:
                    raise ValueError(f'invalid physical bigint in {key}')
                value = int(value)
            if value is not None and kind == 'double':
                if isinstance(value, (bool, str)):
                    raise ValueError(f'invalid physical double in {key}')
                value = float(value)
            record[key] = value
        records.append(record)
    return records


def _validate_plan(records, club, pair, proof, batch):
    present = int(proof is None)
    if tuple(len(records[table]) for table in ENTITY) != (1, present, present):
        raise ValueError('invalid profile/stadium/relation counts')
    if proof is not None and proof != _EmptyProof(pair['profile_raw_capture_id'], 200,
                                                pair['stadium_raw_capture_id'], 404):
        raise ValueError('invalid paired raw HTTP 404 proof')
    for table, rows in records.items():
        for item in rows:
            if any(item[key] is None for key in (*LINEAGE, *METADATA, *KEYS[table])):
                raise ValueError('incomplete club stadium lineage or natural key')
            endpoint = 'profile' if table == CLUB_PROFILE_TABLE else 'stadium'
            if (item['club_id'] != club or item['_batch_id'] != batch
                    or item['_source'] != 'transfermarkt' or item['_entity_type'] != ENTITY[table]
                    or item['observed_at'] != pair[f'{endpoint}_observed_at']
                    or item['captured_at'] != item['observed_at']
                    or item['raw_capture_id'] != pair[f'{endpoint}_raw_capture_id']
                    or item['raw_body_hash'] != pair[f'{endpoint}_raw_body_hash']
                    or any(item[key] != value for key, value in pair.items())
                    or item['extracted_at'] != max(pair['profile_observed_at'],
                                                   pair['stadium_observed_at'])
                    or item['_ingested_at'] != item['extracted_at']):
                raise ValueError('inconsistent club stadium lineage')
    if present:
        venue, relation = records[STADIUM_TABLE][0], records[RELATION_TABLE][0]
        if (any(venue[key] != relation[key] for key in (*VENUE_KEYS, 'observed_at', 'status'))
                or venue['status'] != 'observed_present' or relation['valid_from'] is not None
                or relation['valid_to'] is not None):
            raise ValueError('inconsistent club stadium relation')


def _select(connection, table, columns, where, params):
    sql = f'SELECT {", ".join(columns)} FROM {table} WHERE {where}'
    frame = current.run._query_dataframe(connection, sql, list(params))
    if frame.empty:
        return []
    return _records(frame, {column: SCHEMAS[table][column] for column in columns})


def _lookup(connection, table, keys):
    where = ' AND '.join(f'{column} = ?' for column in keys)
    return _select(connection, table, SCHEMAS[table], where, keys.values())


def _check_freshness(connection, club, pair):
    columns = ('profile_observed_at', 'profile_raw_body_hash',
               'stadium_observed_at', 'stadium_raw_body_hash')
    for table in SCHEMAS:
        history = _select(connection, table, columns,
                          'club_id = ? AND (profile_observed_at >= ? OR stadium_observed_at >= ?)',
                          (club, pair['profile_observed_at'], pair['stadium_observed_at']))
        for previous in history:
            for endpoint in ('profile', 'stadium'):
                clock = endpoint + '_observed_at'
                body = endpoint + '_raw_body_hash'
                if previous[clock] > pair[clock]:
                    raise ValueError(f'stale {endpoint} capture for club')
                if previous[clock] == pair[clock] and previous[body] != pair[body]:
                    raise ValueError(f'conflicting {endpoint} hash at equal observed_at')


def _merge(scraper, table, frame, preflight):
    _, database, name = table.split('.')
    _authorize(preflight)
    result = scraper._iceberg_writer.write_dataframe(
        df=frame, database=database, table=name, merge_keys=list(KEYS[table]),
        add_metadata=False, source='transfermarkt',
    )
    if result is False:
        raise ValueError(f'native merge failed for {table}')


def _read_business(scraper, connection, table, frame, expected, batch):
    physical = _lookup(connection, table, {'_batch_id': batch})
    if _json(physical) != _json(expected):
        raise ValueError(f'batch physical readback mismatch for {table}')
    if not expected:
        return _digest([])
    keys = {key: expected[0][key] for key in KEYS[table]}
    if _json(_lookup(connection, table, keys)) != _json(expected):
        raise ValueError(f'natural key physical readback mismatch for {table}')
    return _digest(physical)


def _persist(scraper, connection, preflight, club, profile, stadium, frames, batch,
             context, pair, proof):
    records = {table: _records(frame, SCHEMAS[table]) for table, frame in frames.items()}
    _validate_plan(records, club, pair, proof, batch)
    key_maps = {table: [{key: row[key] for key in KEYS[table]} for row in rows]
                for table, rows in records.items()}
    status = 'observed_empty' if proof is not None else 'observed_present'
    plan = {
        'receipt_id': _digest(['club_stadium_receipt_v1', batch]), 'club_id': club,
        **context, **pair, 'profile_source_url': profile.url, 'stadium_source_url': stadium.url,
        'profile_status_code': profile.status_code, 'stadium_status_code': stadium.status_code,
        'stadium_status': status, 'empty_proof_json': _json(asdict(proof)) if proof else None,
        'key_maps_json': _json(key_maps), 'expected_profile_count': len(records[CLUB_PROFILE_TABLE]),
        'expected_stadium_count': len(records[STADIUM_TABLE]),
        'expected_relation_count': len(records[RELATION_TABLE]),
        'expected_present_count': int(proof is None), 'expected_empty_count': int(proof is not None),
        'batch_id': batch,
    }
    plan['plan_sha256'] = _digest([plan, records])
    for table, schema in SCHEMAS.items():
        ddl = ', '.join(f'{column} {kind}' for column, kind in schema.items())
        current.run._execute_cursor(connection, f'CREATE TABLE IF NOT EXISTS {table} ({ddl})')
    _check_freshness(connection, club, pair)
    receipt_key = {'receipt_id': plan['receipt_id']}
    existing = _lookup(connection, RECEIPTS_TABLE, receipt_key)
    if len(existing) > 1 or (existing and existing[0]['plan_sha256'] != plan['plan_sha256']):
        raise ValueError('existing receipt differs from deterministic plan')
    if not existing:
        missing = []
        for table, rows in records.items():
            if rows:
                previous = _lookup(connection, table, key_maps[table][0])
                if previous and _json(previous) != _json(rows):
                    raise ValueError(f'conflicting immutable observation at {table} natural key')
                if not previous:
                    missing.append(table)
        for table in missing:
            _merge(scraper, table, frames[table], preflight)

    for table, label in zip(ENTITY, ('profile', 'stadium', 'relation'), strict=True):
        plan[label + '_physical_hash'] = _read_business(
            scraper, connection, table, frames[table], records[table], batch)
    committed = existing[0]['committed_at'] if existing else datetime.now(timezone.utc)
    plan['committed_at'] = _clock(committed)
    plan['receipt_sha256'] = _digest(plan)
    if not existing:
        receipt = pd.DataFrame([plan], columns=list(SCHEMAS[RECEIPTS_TABLE]))
        _merge(scraper, RECEIPTS_TABLE, receipt, preflight)
    if _json(_lookup(connection, RECEIPTS_TABLE, receipt_key)) != _json([plan]):
        raise ValueError('receipt physical readback mismatch')
    return {
        **plan,
        'bronze_manifest': batch,
        'committed_at': plan['committed_at'].replace(tzinfo=timezone.utc).isoformat(),
        'verified': True,
    }


@current._bounded_current_writer
def write_current_club_stadium(scraper, scope, preflight, cycle_id, *, club_id,
                               profile_outcome, profile_url, stadium_outcome, stadium_url,
                               country_names, universe_version, run_id):
    deadline = getattr(scraper, '_current_deadline_monotonic', None)
    with writer_deadline(deadline), writer_lock():
        _authorize(preflight)
        resolved = current._scope(scraper, scope)
        club = parser._id(club_id, 'club_id')
        expected = f'https://tmapi.transfermarkt.technology/club/{club}'
        if (not club or any(c in str(club) for c in '/?#%') or profile_url != expected
                or stadium_url != expected + '/stadium'):
            raise ValueError('invalid club profile or stadium URL')
        store = scraper._http_client._raw_store
        profile = _read_capture(store, profile_outcome, profile_url, resolved['scope_id'],
                                allow_404=False)
        stadium = _read_capture(store, stadium_outcome, stadium_url, resolved['scope_id'],
                                allow_404=True)
        if profile.cycle_id != stadium.cycle_id:
            raise ValueError('profile and stadium raw captures belong to different cycles')
        plan = _make_plan(club, profile, stadium, country_names, resolved, cycle_id, run_id,
                          universe_version)
        _authorize(preflight)
        connection = scraper._bronze_connection()
        try:
            return _persist(scraper, connection, preflight, club, profile, stadium, *plan)
        finally:
            connection.close()
