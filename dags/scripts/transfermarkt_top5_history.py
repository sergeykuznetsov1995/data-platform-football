"""Offline SQL and acceptance gates for Transfermarkt top-5 history (#1403)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Sequence

try:
    from dags.utils import transfermarkt_top5_history as top5
except ModuleNotFoundError:
    from utils import transfermarkt_top5_history as top5


def _json_file(path: str) -> Any:
    return json.loads(Path(path).read_text(encoding='utf-8'))


def _pins(path: str) -> dict[str, int]:
    value = _json_file(path)
    if not isinstance(value, dict):
        raise top5.Top5HistoryError('snapshot JSON must be an object')
    return value


def _rows(path: str) -> list[dict[str, Any]]:
    value = _json_file(path)
    if isinstance(value, dict):
        value = value.get('results')
    if not isinstance(value, list) or any(
        not isinstance(row, dict) for row in value
    ):
        raise top5.Top5HistoryError(
            'result JSON must be an array or an object with a results array'
        )
    return value


def _emit(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('matrix')

    coverage_sql = commands.add_parser('coverage-sql')
    coverage_sql.add_argument('--snapshots-json', required=True)

    coverage = commands.add_parser('check-coverage')
    coverage.add_argument('--rows-json', required=True)
    coverage.add_argument('--competition-id')
    coverage.add_argument('--edition-id')
    coverage.add_argument('--gate', choices=('native', 'final'), default='final')

    reconciliation_sql = commands.add_parser('reconciliation-sql')
    reconciliation_sql.add_argument('--competition-id', required=True)
    reconciliation_sql.add_argument('--edition-id', required=True)
    reconciliation_sql.add_argument('--snapshots-json', required=True)

    reconciliation = commands.add_parser('check-reconciliation')
    reconciliation.add_argument('--rows-json', required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        matrix = top5.load_acceptance_matrix()
        if args.command == 'matrix':
            _emit(matrix)
            return 0
        if args.command == 'coverage-sql':
            print(top5.build_coverage_sql(_pins(args.snapshots_json)))
            return 0
        if args.command == 'reconciliation-sql':
            scope = top5.scope_for(args.competition_id, args.edition_id)
            print(top5.build_roster_reconciliation_sql(
                scope, _pins(args.snapshots_json),
            ))
            return 0
        if args.command == 'check-coverage':
            if (args.competition_id is None) != (args.edition_id is None):
                raise top5.Top5HistoryError(
                    'coverage scope requires competition-id and edition-id together'
                )
            scopes = None
            if args.competition_id is not None:
                scopes = (top5.scope_for(args.competition_id, args.edition_id),)
            violations = top5.coverage_violations(
                _rows(args.rows_json), scopes=scopes,
            )
            holds = []
            if args.gate == 'native':
                holds = [item for item in violations if item['kind'] == 'null_scope']
                violations = [item for item in violations if item['kind'] != 'null_scope']
            if violations:
                status = 'FAIL'
            elif args.gate == 'native':
                status = 'NATIVE_PASS_QUARANTINE_HOLD' if holds else 'NATIVE_PASS'
            else:
                status = 'PASS'
            _emit({
                'status': status,
                'scopes': len(scopes or top5.acceptance_scopes()),
                'violations': violations,
                'quarantine_holds': holds,
            })
            return 2 if violations else 0
        if args.command == 'check-reconciliation':
            violations = top5.reconciliation_violations(_rows(args.rows_json))
            _emit({
                'status': 'FAIL' if violations else 'PASS',
                'violations': violations,
            })
            return 2 if violations else 0
        raise top5.Top5HistoryError(f'unknown command: {args.command}')
    except (OSError, json.JSONDecodeError, top5.Top5HistoryError) as exc:
        print(f'error: {exc}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
