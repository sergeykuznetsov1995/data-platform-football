#!/usr/bin/env python3
"""Durable, evidence-backed acceptance ledger. No parser or network operations.

Commands consume JSON from files, never execute commands from a handoff. Miro and
GitHub effects are performed by the authorized lead and acknowledged by readback.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import uuid


SOURCES = {'fotmob', 'sofascore', 'transfermarkt', 'fbref', 'whoscored',
           'espn', 'clubelo', 'understat'}
REPOSITORY = 'https://github.com/sergeykuznetsov1995/data-platform-football'


class AcceptanceError(ValueError):
    """A supplied assertion cannot safely advance acceptance."""


def require(condition, message):
    if not condition:
        raise AcceptanceError(message)


def instant(value):
    try:
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        require(value.tzinfo is not None, 'timestamp requires a timezone')
        return value.astimezone(timezone.utc)
    except (AttributeError, TypeError, ValueError) as exc:
        raise AcceptanceError('invalid timestamp') from exc


def clock(value=None):
    return instant(value) if value else datetime.now(timezone.utc)


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def number(value, minimum=0):
    return type(value) is int and value >= minimum


class Store:
    def __init__(self, root, *, evidence_roots=None, sources=None):
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.database = self.root / 'acceptance.sqlite3'
        self.evidence_roots = [Path(p).resolve() for p in (
            evidence_roots or ['/root/data-platform-football', self.root])]
        self.sources = sources or {}
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS records(id TEXT PRIMARY KEY, revision INTEGER NOT NULL,
                                                 data TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS events(sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    acceptance_id TEXT NOT NULL, series INTEGER NOT NULL, kind TEXT NOT NULL,
                    payload TEXT NOT NULL, at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS handoffs(source TEXT PRIMARY KEY, data TEXT NOT NULL);
            ''')

    def connect(self):
        db = sqlite3.connect(self.database, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA foreign_keys=ON')
        db.execute('PRAGMA synchronous=FULL')
        return db

    @contextmanager
    def transaction(self):
        db = self.connect()
        try:
            db.execute('BEGIN IMMEDIATE')
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    def _load(self, db, key):
        row = db.execute('SELECT * FROM records WHERE id=?', (key,)).fetchone()
        require(row is not None, 'acceptance not registered')
        record = json.loads(row['data'])
        record['revision'] = row['revision']
        return record

    def _save(self, db, record, kind, payload, now):
        cursor = db.execute('INSERT INTO events(acceptance_id,series,kind,payload,at) VALUES(?,?,?,?,?)',
            (record['id'], record['series'], kind, encode(payload), now.isoformat()))
        record['revision'] = cursor.lastrowid
        db.execute('INSERT INTO records VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET '
                   'revision=excluded.revision,data=excluded.data',
                   (record['id'], record['revision'], encode(record)))

    def _proof(self, paths):
        require(isinstance(paths, list), 'evidence must be a list of local paths')
        result = []
        for raw in paths:
            require(isinstance(raw, str), 'evidence path must be text')
            path = Path(raw).resolve(strict=True)
            require(any(path.is_relative_to(root) for root in self.evidence_roots),
                    'evidence outside permitted project roots; copy a sanitized report first')
            require(path.is_file() and 0 < path.stat().st_size <= 16 * 1024 * 1024,
                    'evidence must be a nonempty file up to 16 MiB')
            require(not any(part.startswith('.env') for part in path.parts), 'credentials are not evidence')
            data = path.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            saved = self.root / 'evidence' / digest
            saved.parent.mkdir(exist_ok=True)
            try:
                with saved.open('xb') as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
                directory_fd = os.open(saved.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except FileExistsError:
                require(hashlib.sha256(saved.read_bytes()).hexdigest() == digest,
                        'saved evidence fingerprint mismatch')
            result.append({'path': str(path), 'sha256': digest, 'snapshot': str(saved)})
        return result

    def _validated_spec(self, spec, now):
        spec = json.loads(encode(spec))
        require(spec.get('source') in SOURCES, 'unknown source')
        reference_kind = spec.get('reference_kind', 'issue')
        require(reference_kind in {'issue', 'pull'}, 'unknown reference kind')
        if reference_kind == 'issue':
            require(number(spec.get('issue'), 1), 'issue must be a positive integer')
            require(spec.get('id') == f"{spec['source']}-{spec['issue']}", 'id must be source-issue')
        else:
            require(spec.get('issue') is None and number(spec.get('pull_request'), 1), 'PR acceptance requires a PR number and no issue')
            require(spec.get('id') == f"{spec['source']}-pr-{spec['pull_request']}", 'id must be source-pr-number')
            require(spec.get('reference_url') == f"{REPOSITORY}/pull/{spec['pull_request']}", 'invalid PR reference')
            require(spec.get('auto_close_issue') is False, 'PR acceptance cannot close a GitHub issue')
        require(isinstance(spec.get('title'), str) and spec['title'].strip(), 'title required')
        require(re.fullmatch('[0-9a-f]{64}', spec.get('issue_body_sha256', '')),
                'issue definition fingerprint required')
        require(type(spec.get('definition_confirmed')) is bool, 'definition confirmation required')
        criteria = spec.get('criteria')
        require(isinstance(criteria, list) and criteria, 'nonempty full criteria list required')
        identifiers = set()
        for criterion in criteria:
            cid = criterion.get('id', '')
            require(isinstance(cid, str) and re.fullmatch('[a-z0-9_-]{1,80}', cid)
                    and cid not in identifiers, 'unique criterion id required')
            identifiers.add(cid)
            require(criterion.get('kind') in {'days', 'runs', 'check'}, 'unsupported criterion kind')
            require(isinstance(criterion.get('title'), str) and criterion['title'].strip(), 'criterion title required')
            require(number(criterion.get('required'), 1), 'positive criterion target required')
            require(criterion['kind'] != 'check' or criterion['required'] == 1, 'check target must be one')
            criterion.setdefault('min_samples', 1)
            require(number(criterion['min_samples']), 'invalid minimum sample size')
            if criterion['kind'] == 'days':
                criterion.setdefault('period_seconds', 86400)
                require(number(criterion['period_seconds'], 1), 'positive period required')
        if spec.get('started_at'):
            require(instant(spec['started_at']) <= now, 'acceptance start is in the future')
            require(bool(spec.get('deployed_revision')), 'deployed revision required')
        return spec

    def register(self, spec, *, now=None):
        now = clock(now)
        spec = self._validated_spec(spec, now)
        with self.transaction() as db:
            previous = db.execute('SELECT data FROM records WHERE id=?', (spec['id'],)).fetchone()
            if previous:
                require(json.loads(previous['data'])['definition'] == spec,
                        'definition changed; register is not an overwrite')
                return self._load(db, spec['id'])
            record = {'id': spec['id'], 'definition': spec, 'series': 1,
                      'started_at': spec.get('started_at'), 'deployed_revision': spec.get('deployed_revision'),
                      'definition_confirmed': spec['definition_confirmed'], 'observations': {},
                      'impact_hold': False, 'closed': False,
                      'reason': '; '.join(spec.get('metadata', {}).get('unknown', [])), 'checked_at': None}
            self._save(db, record, 'registered', spec, now)
            return record

    def revise(self, key, revision, *, now=None):
        """Explicitly replace criteria; preserve old definitions and reset proof."""
        now = clock(now)
        spec = self._validated_spec(revision['spec'], now)
        require(spec['id'] == key, 'revision changes acceptance identity')
        require(isinstance(revision.get('reason'), str) and revision['reason'].strip(), 'revision reason required')
        proof = self._proof(revision.get('evidence', []))
        require(proof, 'definition revision requires evidence')
        with self.transaction() as db:
            record = self._load(db, key)
            require(not record['closed'], 'acceptance already closed')
            require(record['revision'] == revision.get('expected_revision'), 'stale definition revision')
            record.update(definition=spec, series=record['series'] + 1,
                started_at=spec.get('started_at'), deployed_revision=spec.get('deployed_revision'),
                definition_confirmed=spec['definition_confirmed'], observations={}, impact_hold=False,
                checked_at=None, reason=revision['reason'])
            record.pop('close_plan', None)
            self._save(db, record, 'definition_revised',
                       {'spec': spec, 'reason': revision['reason'], 'evidence': proof}, now)
            return record

    def observe(self, key, observation, *, now=None):
        now = clock(now)
        value = json.loads(encode(observation))
        with self.transaction() as db:
            record = self._load(db, key)
            require(not record['closed'], 'acceptance already closed')
            require(value.get('series') == record['series'], 'stale series')
            require(record.get('started_at'), 'acceptance has not started')
            criteria = {c['id']: c for c in record['definition']['criteria']}
            criterion = criteria.get(value.get('criterion'))
            require(criterion is not None, 'unknown criterion')
            require(value.get('status') in {'pass', 'fail', 'unknown'}, 'invalid observation status')
            require(number(value.get('ordinal')), 'nonnegative ordinal required')
            require(isinstance(value.get('key'), str) and value['key'].strip(), 'observation key required')
            start, end, checked = (instant(value.get(k)) for k in ['start', 'end', 'checked_at'])
            require(instant(record['started_at']) <= start <= end <= checked <= now,
                    'observation interval is immature or before delivery')
            if criterion['kind'] == 'days':
                expected = instant(record['started_at']) + timedelta(
                    seconds=criterion['period_seconds'] * value['ordinal'])
                require(start == expected and end == expected + timedelta(seconds=criterion['period_seconds']),
                        'day interval does not match acceptance definition')
            if criterion['kind'] == 'check':
                require(value['ordinal'] == 0, 'one-off check ordinal must be zero')
            require(number(value.get('samples')), 'sample count required')
            require(value['status'] != 'pass' or value['samples'] >= criterion['min_samples'],
                    'insufficient sample for successful result')
            require(isinstance(value.get('reason'), str) and value['reason'].strip(), 'observation reason required')
            value['evidence'] = self._proof(value.get('evidence', []))
            require(value['status'] == 'unknown' or value['evidence'], 'pass/fail requires evidence')
            observations = record['observations'].setdefault(criterion['id'], {})
            slot = str(value['ordinal'])
            for old_slot, old in observations.items():
                require(old_slot == slot or old['key'] != value['key'], 'run key already counted')
            previous = observations.get(slot)
            if previous:
                require(previous['key'] == value['key'] and previous['start'] == value['start']
                        and previous['end'] == value['end'], 'observation identity changed')
                if previous == value:
                    return record
                require(checked >= instant(previous['checked_at']), 'stale observation')
                require(previous['status'] != 'fail' or value['status'] == 'fail',
                        'a failed interval cannot be repainted; start a new series')
                require(not (previous['status'] == 'pass' and value['status'] == 'unknown'),
                        'a failed read cannot erase an established result')
            observations[slot] = value
            record['checked_at'] = max(record['checked_at'] or value['checked_at'], value['checked_at'], key=instant)
            record['reason'] = value['reason']
            record.pop('close_plan', None)
            self._save(db, record, 'observation', value, now)
            return record

    def change(self, key, decision, *, now=None):
        now = clock(now)
        decision = json.loads(encode(decision))
        with self.transaction() as db:
            record = self._load(db, key)
            require(not record['closed'], 'acceptance already closed')
            require(decision.get('expected_series') == record['series'], 'stale series')
            require(decision.get('expected_revision') == record['revision'], 'stale impact assessment')
            require(decision.get('impact') in {'affected', 'unaffected', 'unknown'}, 'impact assessment required')
            require(isinstance(decision.get('reason'), str) and decision['reason'].strip(), 'impact reason required')
            require(isinstance(decision.get('deployed_revision'), str) and decision['deployed_revision'].strip(),
                    'deployed revision required')
            decision['evidence'] = self._proof(decision.get('evidence', []))
            require(decision['evidence'], 'impact evidence required')
            record['impact_hold'] = decision['impact'] == 'unknown'
            if decision['impact'] == 'affected':
                require(instant(decision.get('started_at')) <= now, 'future acceptance start')
                if record['started_at']:
                    require(instant(decision['started_at']) >= instant(record['started_at']), 'start cannot move backwards')
                    record['series'] += 1
                record['started_at'] = decision['started_at']
                record['observations'] = {}
                record['checked_at'] = None
            record['deployed_revision'] = decision['deployed_revision']
            record['reason'] = decision['reason']
            record.pop('close_plan', None)
            self._save(db, record, 'change', decision, now)
            return record

    def confirm_definition(self, key, body_sha256, evidence, *, now=None):
        now = clock(now)
        with self.transaction() as db:
            record = self._load(db, key)
            require(body_sha256 == record['definition']['issue_body_sha256'], 'issue definition changed')
            proof = self._proof(evidence)
            require(proof, 'full criteria verification evidence required')
            require(not record['closed'], 'acceptance already closed')
            if record['definition_confirmed']:
                return record
            record['definition_confirmed'] = True
            record.pop('close_plan', None)
            self._save(db, record, 'definition_confirmed', {'evidence': proof}, now)
            return record

    def handoff(self, source, value, *, now=None):
        now = clock(now)
        require(source in SOURCES, 'unknown source')
        require(instant(value.get('checked_at')) <= now, 'handoff evidence time is in the future')
        require(isinstance(value.get('summary'), str) and value['summary'].strip(), 'handoff summary required')
        value = dict(value)
        expected_revision = value.pop('expected_revision', None)
        require(number(expected_revision), 'handoff expected_revision required; use zero for first record')
        value['evidence'] = self._proof([value['path']])
        value['sha256'] = value['evidence'][0]['sha256']
        value['recorded_at'] = now.isoformat()
        with self.transaction() as db:
            previous = db.execute('SELECT data FROM handoffs WHERE source=?', (source,)).fetchone()
            if previous:
                prior = json.loads(previous['data'])
                require(instant(value['checked_at']) >= instant(prior['checked_at']), 'stale handoff')
                if ({k: v for k, v in value.items() if k not in {'recorded_at', 'revision'}}
                        == {k: v for k, v in prior.items() if k not in {'recorded_at', 'revision'}}):
                    return prior
                require(expected_revision == prior['revision'], 'stale handoff revision')
            else:
                require(expected_revision == 0, 'stale handoff revision')
            cursor = db.execute('INSERT INTO events(acceptance_id,series,kind,payload,at) VALUES(?,0,?,?,?)',
                       (source, 'handoff', encode(value), now.isoformat()))
            value['revision'] = cursor.lastrowid
            db.execute('INSERT INTO handoffs VALUES(?,?) ON CONFLICT(source) DO UPDATE SET data=excluded.data',
                       (source, encode(value)))
        return value

    def _view(self, record, now):
        if record['closed']:
            now = instant(record['accepted_at'])
        definition = record['definition']
        row = {k: record.get(k) for k in ['id', 'revision', 'series', 'deployed_revision', 'checked_at', 'reason']}
        row.update({k: definition[k] for k in ['source', 'issue', 'title']})
        reference_url = (definition['reference_url'] if definition.get('reference_kind') == 'pull'
                         else f"{REPOSITORY}/issues/{definition['issue']}")
        row.update(issue_url=reference_url, criteria=[], days=[], next_check_at=None)
        started = instant(record['started_at']) if record['started_at'] else None
        for criterion in definition['criteria']:
            observations = record['observations'].get(criterion['id'], {})
            last = max((int(x) for x in observations), default=-1)
            if criterion['kind'] == 'days' and started:
                last = max(-1, int((now - started).total_seconds() // criterion['period_seconds']) - 1)
                unchecked = next((n for n in range(last + 1)
                    if observations.get(str(n), {}).get('status', 'unknown') == 'unknown'), last + 1)
                next_check = started + timedelta(seconds=(unchecked + 1) * criterion['period_seconds'])
                row['next_check_at'] = min(row['next_check_at'] or next_check.isoformat(), next_check.isoformat(), key=instant)
            count = 0
            for ordinal in range(last, -1, -1):
                result = observations.get(str(ordinal), {}).get('status', 'unknown')
                if result != 'pass':
                    break
                count += 1
            latest = observations.get(str(last), {}).get('status', 'unknown') if last >= 0 else 'waiting'
            row['criteria'].append({'id': criterion['id'], 'title': criterion['title'],
                'kind': criterion['kind'], 'target': criterion['required'], 'progress': count,
                'status': 'pass' if count >= criterion['required'] else (
                    'waiting' if latest == 'pass' else latest)})
            for ordinal in range(max(0, last - 13), last + 1):
                observation = observations.get(str(ordinal))
                label = str(ordinal + 1)
                if criterion['kind'] == 'days' and started:
                    msk = timezone(timedelta(hours=3))
                    start = started + timedelta(seconds=ordinal * criterion['period_seconds'])
                    end = start + timedelta(seconds=criterion['period_seconds'])
                    label = (start.astimezone(msk).strftime('%d.%m %H:%M') + ' — '
                             + end.astimezone(msk).strftime('%d.%m %H:%M') + ' МСК')
                row['days'].append({'label': f"{criterion['title']}: {label}",
                    'status': observation['status'] if observation else 'unknown'})
        statuses = [c['status'] for c in row['criteria']]
        if record['closed']:
            row['status'] = 'closed'
        elif not record['definition_confirmed']:
            row['status'] = 'draft'
        elif record['impact_hold'] or definition.get('metadata', {}).get('close_hold'):
            row['status'] = 'on_hold'
        elif started is None:
            row['status'] = 'waiting'
        elif all(s == 'pass' for s in statuses):
            row['status'] = 'ready'
        elif 'fail' in statuses:
            row['status'] = 'failed'
        elif 'unknown' in statuses:
            row['status'] = 'unknown'
        else:
            row['status'] = 'observing'
        return row

    def snapshot(self, *, now=None):
        now = clock(now)
        with self.connect() as db:
            db.execute('BEGIN')
            records = [json.loads(r['data']) for r in db.execute('SELECT data FROM records ORDER BY id')]
            handoffs = {r['source']: json.loads(r['data']) for r in db.execute('SELECT * FROM handoffs')}
            revision = db.execute('SELECT COALESCE(MAX(sequence),0) FROM events').fetchone()[0]
            return {'schema': 1, 'revision': revision, 'sources': self.sources, 'handoffs': handoffs,
                    'acceptances': [self._view(r, now) for r in records], 'generated_at': now.isoformat()}

    def history(self, key):
        with self.connect() as db:
            return [{**dict(r), 'payload': json.loads(r['payload'])} for r in db.execute(
                'SELECT * FROM events WHERE acceptance_id=? ORDER BY sequence', (key,))]

    def show(self, key):
        with self.connect() as db:
            return self._load(db, key)

    def prepare_close(self, key, body_sha256, *, now=None):
        now = clock(now)
        with self.transaction() as db:
            record = self._load(db, key)
            require(body_sha256 == record['definition']['issue_body_sha256'], 'issue definition changed')
            require(self._view(record, now)['status'] == 'ready', 'not all acceptance criteria are met')
            for observations in record['observations'].values():
                for observation in observations.values():
                    for proof in observation['evidence']:
                        saved = Path(proof['snapshot'])
                        require(saved.is_file() and hashlib.sha256(saved.read_bytes()).hexdigest() == proof['sha256'],
                                'saved evidence is missing or changed')
            if record.get('close_plan'):
                return record['close_plan']
            plan = {'token': uuid.uuid4().hex, 'id': key, 'issue': record['definition']['issue'],
                    'issue_url': self._view(record, now)['issue_url'],
                    'auto_close_issue': record['definition'].get('reference_kind', 'issue') == 'issue',
                    'acceptance_revision': record['revision'], 'series': record['series'],
                    'body_sha256': body_sha256, 'prepared_at': now.isoformat(),
                    'criteria': self._view(record, now)['criteria']}
            record['close_plan'] = plan
            # Preparing a side effect does not change the evidence revision.
            db.execute('UPDATE records SET data=? WHERE id=?', (encode(record), key))
            return plan

    def confirm_closed(self, key, token, receipt, *, now=None):
        now = clock(now)
        with self.transaction() as db:
            record = self._load(db, key)
            plan = record.get('close_plan')
            require(plan and plan['token'] == token, 'missing or stale close plan')
            if record['closed']:
                require(receipt == record.get('closure_receipt'), 'closure receipt changed')
                return record
            require(plan['acceptance_revision'] == record['revision'], 'acceptance changed after closure preparation')
            if plan['auto_close_issue']:
                require(receipt.get('issue') == plan['issue'] and receipt.get('state') == 'CLOSED'
                        and receipt.get('body_sha256') == plan['body_sha256'], 'GitHub readback does not match plan')
                require(isinstance(receipt.get('report_url'), str)
                        and receipt['report_url'].startswith(plan['issue_url'] + '#issuecomment-'),
                        'published evidence report required')
            else:
                require(receipt.get('state') == 'ACCEPTED' and receipt.get('body_sha256') == plan['body_sha256'],
                        'PR acceptance cannot close or reopen GitHub objects')
                require(self._proof(receipt.get('evidence', [])), 'local final acceptance report required')
            require(self._view(record, now)['status'] == 'ready', 'criteria no longer complete')
            record['closed'] = True
            record['accepted_at'] = now.isoformat()
            record['closure_receipt'] = receipt
            self._save(db, record, 'closed', receipt, now)
            return record


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state', required=True, type=Path)
    parser.add_argument('--sources', type=Path)
    parser.add_argument('--evidence-root', action='append')
    sub = parser.add_subparsers(dest='command', required=True)
    for name in ['register', 'observe', 'change', 'revise', 'handoff', 'confirm-definition', 'confirm-closed']:
        command = sub.add_parser(name)
        command.add_argument('--input', required=True, type=Path)
        if name not in {'register', 'handoff'}:
            command.add_argument('--id', required=True)
    sub.add_parser('snapshot')
    for name in ['history', 'show']:
        command = sub.add_parser(name)
        command.add_argument('--id', required=True)
    close = sub.add_parser('prepare-close')
    close.add_argument('--id', required=True)
    close.add_argument('--body-sha256', required=True)
    args = parser.parse_args(argv)
    sources = json.loads(args.sources.read_text())['sources'] if args.sources else None
    store = Store(args.state, sources=sources, evidence_roots=args.evidence_root)
    value = json.loads(args.input.read_text()) if hasattr(args, 'input') else None
    try:
        if args.command == 'register':
            result = store.register(value)
        elif args.command in {'observe', 'change', 'revise'}:
            result = getattr(store, args.command)(args.id, value)
        elif args.command == 'handoff':
            result = store.handoff(value['source'], value)
        elif args.command == 'confirm-definition':
            result = store.confirm_definition(args.id, value['body_sha256'], value['evidence'])
        elif args.command == 'confirm-closed':
            result = store.confirm_closed(args.id, value['token'], value['receipt'])
        elif args.command == 'prepare-close':
            result = store.prepare_close(args.id, args.body_sha256)
        elif args.command in {'history', 'show'}:
            result = getattr(store, args.command)(args.id)
        else:
            result = store.snapshot()
        print(encode(result))
    except (AcceptanceError, OSError, KeyError) as exc:
        parser.exit(2, f'acceptance: {exc}\n')


if __name__ == '__main__':
    main()
