"""Immutable first observation of a native fact, independent of replacements.

The registry is retained when a physical row is deleted. It is not a migration
of old fetched_at values: only a newly captured, raw-backed row can register a
first observation. Old journals and cached physical captures remain unchanged.
"""
from __future__ import annotations

from datetime import date, datetime
import hashlib
import json

import pandas as pd

from scrapers.transfermarkt.writer import execute_statement, writer_lock

REGISTRY = 'iceberg.ops.transfermarkt_first_seen_v1'
KEYS = {
    'transfermarkt_squad_memberships': ('competition_id', 'edition_id', 'club_id', 'player_id'),
    'transfermarkt_market_value_points': ('player_id', 'mv_date'),
    'transfermarkt_transfer_events': ('transfer_id',),
    'transfermarkt_coach_stints': ('club_id', 'coach_id', 'appointed_date', 'left_date'),
}


def _cell(value):
    if pd.isna(value):
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def fact_key(table, row):
    """Canonical, NULL-safe existing natural key; never uses batch or scope."""
    values = [None if pd.isna(row[key]) else pd.Timestamp(row[key]).date().isoformat()
              if key in {'mv_date', 'appointed_date', 'left_date'} else _cell(row[key])
              for key in KEYS[table]]
    encoded = json.dumps(values, separators=(',', ':'))
    return hashlib.sha256(encoded.encode()).hexdigest(), encoded


def _time(value):
    stamp = pd.Timestamp(value)
    if pd.isna(stamp):
        raise ValueError('first_seen_at requires a real source observation time')
    return (stamp.tz_localize('UTC') if stamp.tzinfo is None else stamp.tz_convert('UTC')).to_pydatetime()


def _read(cur, table, hashes):
    found = {}
    for start in range(0, len(hashes), 250):
        part = hashes[start:start + 250]
        execute_statement(cur, f'SELECT key_hash, key_json, first_seen_at FROM {REGISTRY} '
                          f'WHERE table_name = ? AND key_hash IN ({", ".join("?" for _ in part)})', (table, *part))
        for digest, key, stamp in cur.fetchall():
            if digest in found:
                raise RuntimeError('first_seen registry has duplicate fact keys')
            found[digest] = (key, _time(stamp))
    return found


def inherit_registered_clocks(scraper, output, frame):
    """Keep known metadata when an old journal writes its original projection.

    This reads existing clocks only. The old parsed frame, intent hash, raw,
    batch and historical receipt projection remain unchanged. Unknown old
    facts are not registered or dated by replay.
    """
    if (output.is_legacy or output.table_name not in KEYS or frame is None
            or frame.empty or 'first_seen_at' in frame):
        return frame
    keys = [fact_key(output.table_name, row) for row in frame.to_dict('records')]
    connection = scraper._bronze_connection()
    cur = connection.cursor()
    try:
        try:
            stored = _read(cur, output.table_name, sorted({key[0] for key in keys}))
        except Exception as exc:
            if any(token in str(exc).lower() for token in ('table_not_found', 'table not found', 'no such table')):
                return frame
            raise
        if not stored:
            return frame
        for digest, encoded in keys:
            if digest in stored and stored[digest][0] != encoded:
                raise RuntimeError('first_seen natural-key hash collision')
        inherited = frame.copy()
        inherited['first_seen_at'] = pd.to_datetime(
            [stored[digest][1] if digest in stored else pd.NaT for digest, _ in keys], utc=True)
        return inherited
    finally:
        cur.close()
        connection.close()


def prepare_frames(scraper, outputs, frames):
    """Resolve immutable clocks before save_intent and any Bronze mutation.

    Cached source IDs keep their physical columns/clocks. All newly captured
    rows require original source lineage. An ambiguous SQL commit is recovered
    by looking up the registry on the next invocation, never by overwriting it.
    """
    updated = dict(frames)
    relevant = [output for output in outputs if not output.is_legacy and output.table_name in KEYS
                and output.key in frames and frames[output.key] is not None and not frames[output.key].empty]
    if not relevant:
        return updated
    with writer_lock():
        connection = scraper._bronze_connection()
        cur = connection.cursor()
        try:
            execute_statement(cur, f'''CREATE TABLE IF NOT EXISTS {REGISTRY} (
table_name varchar, key_hash varchar, key_json varchar, first_seen_at timestamp(6),
source_url varchar, source_body_hash varchar, cycle_id varchar
) WITH (format = 'PARQUET')''')
            for output in relevant:
                frame = frames[output.key]
                cached = {str(ref['player_id']) for ref in frame.attrs.get('tm_original_capture_refs', [])}
                captured = frame.attrs.get('tm_captured_player_ids')
                active = [index for index, row in enumerate(frame.to_dict('records'))
                          if ('player_id' not in row or str(row['player_id']) not in cached)
                          and (captured is None or str(row.get('player_id')) in set(map(str, captured)))]
                if not active:
                    continue
                rows = frame.to_dict('records')
                keys = {index: fact_key(output.table_name, rows[index]) for index in active}
                hashes = sorted({value[0] for value in keys.values()})
                stored = _read(cur, output.table_name, hashes)
                additions = {}
                for index in active:
                    row = rows[index]
                    digest, encoded = keys[index]
                    if digest in stored:
                        if stored[digest][0] != encoded:
                            raise RuntimeError('first_seen natural-key hash collision')
                        stamp = stored[digest][1]
                        supplied = row.get('first_seen_at')
                        if supplied is not None and not pd.isna(supplied) and _time(supplied) != stamp:
                            raise RuntimeError('immutable first_seen_at differs from registered observation')
                    else:
                        if any(not row.get(field) or pd.isna(row[field]) for field in ('source_url', 'source_body_hash', 'fetched_at')):
                            raise RuntimeError('new first_seen observation lacks source lineage')
                        stamp = _time(row['fetched_at'])
                        supplied = row.get('first_seen_at')
                        if supplied is not None and not pd.isna(supplied) and _time(supplied) != stamp:
                            raise RuntimeError('unregistered first_seen_at is not the original capture time')
                        # Duplicate physical rows of the same key must agree on
                        # their original capture, not depend on frame ordering.
                        value = (output.table_name, digest, encoded, stamp.replace(tzinfo=None),
                                 row['source_url'], row['source_body_hash'], row.get('cycle_id'))
                        if digest in additions and additions[digest][3] != value[3]:
                            raise RuntimeError('one fact has conflicting first observation times')
                        additions[digest] = value
                values = list(additions.values())
                for start in range(0, len(values), 250):
                    part = values[start:start + 250]
                    sql = f'INSERT INTO {REGISTRY} (table_name, key_hash, key_json, first_seen_at, source_url, source_body_hash, cycle_id) VALUES '
                    sql += ', '.join('(' + ', '.join('?' for _ in item) + ')' for item in part)
                    execute_statement(cur, sql, tuple(cell for item in part for cell in item))
                # A registry write is usable only after physical read-back.
                verified = _read(cur, output.table_name, hashes)
                if set(verified) != set(hashes):
                    raise RuntimeError('first_seen registry read-back is incomplete')
                clocks = list(frame['first_seen_at']) if 'first_seen_at' in frame else [pd.NaT] * len(frame)
                for index in active:
                    digest, encoded = keys[index]
                    if verified[digest][0] != encoded:
                        raise RuntimeError('first_seen registry read-back identity differs')
                    clocks[index] = verified[digest][1]
                enriched = frame.copy()
                enriched['first_seen_at'] = pd.to_datetime(clocks, utc=True)
                updated[output.key] = enriched
        finally:
            cur.close()
            connection.close()
    return updated
