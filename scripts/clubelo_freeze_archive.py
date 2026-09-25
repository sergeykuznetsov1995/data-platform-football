#!/usr/bin/env python3
"""One-off freeze of the ClubElo bronze archive (#1460).

api.clubelo.com is closed since 09.2026, so the three legacy bronze tables
cannot be re-collected. This script only ADDS new tables; the three source
tables are read, never written:

  copy      CREATE TABLE IF NOT EXISTS iceberg.bronze.clubelo_<t>_archive_20260924
            AS SELECT * FROM iceberg.bronze.clubelo_<t>  (three safety copies),
            then compares count(*) / max(_ingested_at) of source vs copy.
  load-csv  loads the 88 legacy API CSV snapshots (soccerdata cache, copied to
            the host) into the new iceberg.bronze.clubelo_api_snapshot_archive
            in one batch; refuses to write if that table is not empty.
  verify    prints source/copy counts and the snapshot-archive totals.

Invariant (unit-tested on this file's text): the only data-modifying
statements are CTAS into the *_archive_20260924 copies and row inserts into
clubelo_api_snapshot_archive.

Usage (from the host, Trino published on 127.0.0.1:8082):
    export TRINO_HOST=127.0.0.1 TRINO_PORT=8082 TRINO_USER=airflow TRINO_PASSWORD=...
    python scripts/clubelo_freeze_archive.py copy --dry-run
    python scripts/clubelo_freeze_archive.py copy
    python scripts/clubelo_freeze_archive.py load-csv \\
        --dir /root/clubelo-archive/soccerdata-cache-20260924
    python scripts/clubelo_freeze_archive.py verify
"""

import argparse
import datetime as dt
import hashlib
import os
import sys
import warnings
from pathlib import Path

import pandas as pd
import trino

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

SCHEMA = 'iceberg.bronze'
SOURCE_TABLES = ('clubelo_ratings', 'clubelo_ratings_historical', 'clubelo_team_history')
COPY_SUFFIX = '_archive_20260924'
SNAPSHOT_TABLE = f'{SCHEMA}.clubelo_api_snapshot_archive'
BATCH_ID = 'clubelo-archive-20260924'
MODEL_VERSION = 'api-legacy'
SOURCE = 'clubelo_api_cache'
INSERT_CHUNK = 500
CSV_COLUMNS = ['Rank', 'Club', 'Country', 'Level', 'Elo', 'From', 'To']
# The frozen cache: exactly 88 weekly/daily files 2025-07-13 .. 2026-08-31 (#1460).
EXPECTED_DATES = 88
FIRST_DATE = '2025-07-13'
LAST_DATE = '2026-08-31'
EXPECTED_ROWS = 54010         # sum of the 88 CSVs, pinned by SHA256SUMS
MIN_ROWS_PER_DATE = 500       # measured 583..630 clubs per date
NULL_MARKERS = ('', 'None')  # the API wrote the literal "None" for unranked clubs

SNAPSHOT_DDL = f'''CREATE TABLE IF NOT EXISTS {SNAPSHOT_TABLE} (
    "rank" bigint,
    "club" varchar,
    "country" varchar,
    "level" bigint,
    "elo" double,
    "valid_from" date,
    "valid_to" date,
    "rating_date" varchar,
    "source_file" varchar,
    "model_version" varchar,
    "_source" varchar,
    "_ingested_at" timestamp(6),
    "_batch_id" varchar
)'''
SNAPSHOT_COLUMNS = (
    'rank', 'club', 'country', 'level', 'elo', 'valid_from', 'valid_to',
    'rating_date', 'source_file', 'model_version', '_source', '_ingested_at', '_batch_id',
)


def _connect():
    user = os.environ.get('TRINO_USER', 'airflow')
    return trino.dbapi.connect(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', 8443)),
        user=user,
        catalog='iceberg',
        http_scheme='https',
        auth=trino.auth.BasicAuthentication(user, os.environ.get('TRINO_PASSWORD', '')),
        verify=False,
    )


def _q(cur, sql):
    cur.execute(sql)
    return cur.fetchall()


def copy_statements():
    return [
        f'CREATE TABLE IF NOT EXISTS {SCHEMA}.{t}{COPY_SUFFIX} AS SELECT * FROM {SCHEMA}.{t}'
        for t in SOURCE_TABLES
    ]


def _stats(cur, table):
    count, max_ingested = _q(cur, f'SELECT count(*), max(_ingested_at) FROM {table}')[0]
    return count, max_ingested


def compare_copies(cur):
    """[(table, src_stats, copy_stats)] for the three sources and their copies."""
    return [
        (t, _stats(cur, f'{SCHEMA}.{t}'), _stats(cur, f'{SCHEMA}.{t}{COPY_SUFFIX}'))
        for t in SOURCE_TABLES
    ]


def cmd_copy(args):
    stmts = copy_statements()
    if args.dry_run:
        for s in stmts:
            print(s + ';')
        return 0
    cur = _connect().cursor()
    for s in stmts:
        _q(cur, s)
        print(f'ok: {s}')
    bad = 0
    for t, src, cp in compare_copies(cur):
        status = 'OK' if src == cp else 'MISMATCH'
        bad += status != 'OK'
        print(f'{status}: {t} source={src} copy={cp}')
    if bad:
        print(f'ERROR: {bad} copy/source mismatch(es)', file=sys.stderr)
        return 1
    return 0


def _null_or(value, cast):
    return None if value in NULL_MARKERS else cast(value)


def parse_csv_file(path, ingested_at):
    """Rows (tuples in SNAPSHOT_COLUMNS order) of one legacy API CSV snapshot."""
    path = Path(path)
    rating_date = dt.date.fromisoformat(path.stem).isoformat()
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    if list(df.columns) != CSV_COLUMNS:
        raise ValueError(f'{path.name}: unexpected header {list(df.columns)}')
    rows = []
    for r in df.itertuples(index=False):
        rows.append((
            _null_or(r.Rank, int),
            r.Club,
            r.Country,
            int(r.Level),
            float(r.Elo),
            dt.date.fromisoformat(r.From),
            _null_or(r.To, dt.date.fromisoformat),  # 2026-12-31 is a site placeholder, kept as is
            rating_date,
            path.name,
            MODEL_VERSION,
            SOURCE,
            ingested_at,
            BATCH_ID,
        ))
    return rows


def check_snapshot_files(files, directory):
    """Error text if the file set is not the frozen 88-date cache, else None.

    Besides count and range, every file must match the ``SHA256SUMS`` written
    when the cache was copied off the container (same name set, same bytes).
    """
    stems = sorted(f.stem for f in files)
    if len(stems) != EXPECTED_DATES or stems[0] != FIRST_DATE or stems[-1] != LAST_DATE:
        got = f'{len(stems)} files {stems[0]}..{stems[-1]}' if stems else '0 files'
        return f'expected {EXPECTED_DATES} files {FIRST_DATE}..{LAST_DATE}, got {got}'
    sums_path = Path(directory) / 'SHA256SUMS'
    if not sums_path.is_file():
        return 'SHA256SUMS missing'
    expected = {}
    for line in sums_path.read_text().splitlines():
        digest, name = line.split(maxsplit=1)
        expected[name.lstrip('*')] = digest
    actual = {f.name: hashlib.sha256(f.read_bytes()).hexdigest() for f in files}
    if actual != expected:
        bad = sorted(set(actual) ^ set(expected)) + sorted(
            n for n in set(actual) & set(expected) if actual[n] != expected[n])
        return f'SHA256SUMS mismatch: {bad[:5]}'
    return None


def parse_csv_dir(directory, ingested_at):
    files = sorted(Path(directory).glob('*.csv'))
    rows = []
    for f in files:
        rows.extend(parse_csv_file(f, ingested_at))
    return files, rows


def _lit(v):
    if v is None:
        return 'NULL'
    if isinstance(v, bool):
        raise TypeError('bool literal not expected')
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return f'{v!r}E0' if 'e' not in repr(v) else repr(v)
    if isinstance(v, dt.datetime):
        return f"TIMESTAMP '{v.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    if isinstance(v, dt.date):
        return f"DATE '{v.isoformat()}'"
    return "'" + str(v).replace("'", "''") + "'"


def insert_statements(rows):
    cols = ', '.join(f'"{c}"' for c in SNAPSHOT_COLUMNS)
    stmts = []
    for i in range(0, len(rows), INSERT_CHUNK):
        values = ',\n'.join(
            '(' + ', '.join(_lit(v) for v in row) + ')' for row in rows[i:i + INSERT_CHUNK]
        )
        stmts.append(f'INSERT INTO {SNAPSHOT_TABLE} ({cols}) VALUES\n{values}')
    return stmts


def load_csv(cur, rows, n_files):
    """Create the snapshot table and load rows in one batch; refuse if non-empty."""
    _q(cur, SNAPSHOT_DDL)
    existing = _q(cur, f'SELECT count(*) FROM {SNAPSHOT_TABLE}')[0][0]
    if existing:
        state = 'complete' if existing == len(rows) else 'PARTIAL (interrupted load?)'
        print(
            f'ERROR: {SNAPSHOT_TABLE} already has {existing} rows of {len(rows)} '
            f'expected — {state}; one-batch load refused. This script never removes '
            'rows: a repeat/cleanup is an owner decision.', file=sys.stderr,
        )
        return 1
    for s in insert_statements(rows):
        _q(cur, s)
    count, dates = _q(cur, f'SELECT count(*), count(DISTINCT rating_date) FROM {SNAPSHOT_TABLE}')[0]
    print(f'loaded: rows={count} (csv {len(rows)}), dates={dates} (files {n_files})')
    if count != len(rows) or dates != n_files:
        print('ERROR: loaded totals differ from the CSV totals', file=sys.stderr)
        return 1
    return 0


def cmd_load_csv(args):
    ingested_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    files, rows = parse_csv_dir(args.dir, ingested_at)
    error = check_snapshot_files(files, args.dir)
    if error:
        print(f'ERROR: {args.dir}: {error}', file=sys.stderr)
        return 1
    print(f'parsed: files={len(files)} rows={len(rows)} '
          f'dates={files[0].stem}..{files[-1].stem}')
    if args.dry_run:
        print(SNAPSHOT_DDL + ';')
        stmts = insert_statements(rows)
        print(f'-- {len(stmts)} INSERT statements of <= {INSERT_CHUNK} rows; first row:')
        print(stmts[0].split('\n')[0] + '\n' + stmts[0].split('\n')[1].rstrip(',') + ' ...')
        return 0
    return load_csv(_connect().cursor(), rows, len(files))


def cmd_verify(args):
    cur = _connect().cursor()
    print('| table | source count | source max(_ingested_at) | copy count | copy max(_ingested_at) |')
    print('|---|---|---|---|---|')
    problems = []
    for t, src, cp in compare_copies(cur):
        print(f'| {t} | {src[0]} | {src[1]} | {cp[0]} | {cp[1]} |')
        if src != cp:
            problems.append(f'{t}: copy differs from source')
    count, dates, dmin, dmax, clubs, min_per_date, avg_per_date = _q(cur, f'''
        SELECT sum(n), count(*), min(rating_date), max(rating_date),
               (SELECT count(DISTINCT club) FROM {SNAPSHOT_TABLE}),
               min(n), round(avg(n), 1)
        FROM (SELECT rating_date, count(*) AS n FROM {SNAPSHOT_TABLE} GROUP BY rating_date)''')[0]
    print(f'\n{SNAPSHOT_TABLE}: rows={count} dates={dates} ({dmin}..{dmax}) '
          f'distinct_clubs={clubs} min_rows_per_date={min_per_date} '
          f'avg_rows_per_date={avg_per_date}')
    if (dates, dmin, dmax) != (EXPECTED_DATES, FIRST_DATE, LAST_DATE):
        problems.append(f'snapshot archive: {dates} dates {dmin}..{dmax}, expected '
                        f'{EXPECTED_DATES} {FIRST_DATE}..{LAST_DATE}')
    if count != EXPECTED_ROWS or (min_per_date or 0) < MIN_ROWS_PER_DATE:
        problems.append(f'snapshot archive: {count} rows (expected {EXPECTED_ROWS}), '
                        f'min {min_per_date} rows per date (expected >= {MIN_ROWS_PER_DATE})')
    for p in problems:
        print(f'ERROR: {p}', file=sys.stderr)
    return 1 if problems else 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    sub = ap.add_subparsers(dest='cmd', required=True)
    p = sub.add_parser('copy')
    p.add_argument('--dry-run', action='store_true')
    p.set_defaults(func=cmd_copy)
    p = sub.add_parser('load-csv')
    p.add_argument('--dir', required=True)
    p.add_argument('--dry-run', action='store_true')
    p.set_defaults(func=cmd_load_csv)
    p = sub.add_parser('verify')
    p.set_defaults(func=cmd_verify)
    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == '__main__':
    sys.exit(main())
