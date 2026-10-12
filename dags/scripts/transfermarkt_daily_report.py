#!/usr/bin/env python3
"""Render a daily SELECT or import evidence already exported by an operator.

No site requests, Trino connection, writer, scheduler or watchdog mutation.
SQL mode needs promoted-registry rows; report mode needs the SELECT result.
"""
from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timezone
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / 'dags') not in sys.path:
    sys.path.append(str(ROOT / 'dags'))

def main(argv=None) -> int:
    from dags.utils.transfermarkt_daily_measure import (
        VERSION, MeasurementError, build_daily_measure_sql, daily_denominator, day_bounds,
        format_daily_summary, import_site_sample, utc,
    )
    from scrapers.transfermarkt.denominator import denominator_path, load_denominator

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--day', required=True, help='YYYY-MM-DD, Moscow calendar day')
    parser.add_argument('--as-of', required=True, help='ISO8601 including timezone')
    parser.add_argument('--registry-json', type=Path, help='promoted scope query rows; SQL mode')
    parser.add_argument('--denominator', type=Path, default=None)
    parser.add_argument('--report-json', type=Path, help='one daily SELECT result object')
    parser.add_argument('--samples-json', type=Path, help='offline sample list; paths relative to its directory')
    parser.add_argument('--dq-json', type=Path, help='same-day daily Bronze DQ result')
    parser.add_argument('--dq-state-root', type=Path, help='read existing daily DQ result; does not run DQ')
    args = parser.parse_args(argv)
    if bool(args.registry_json) == bool(args.report_json):
        parser.error('choose exactly one of --registry-json and --report-json')
    if args.registry_json:
        path = args.denominator or denominator_path()
        denominator = daily_denominator(load_denominator(path).rows.values(), json.loads(args.registry_json.read_text()),
                                        denominator_version=hashlib.sha256(path.read_bytes()).hexdigest())
        sql, params = build_daily_measure_sql(denominator, args.day, as_of=args.as_of)
        # Safe bindings are explicit; no shell substitution or SQL identifier interpolation.
        print(json.dumps({'sql': sql, 'params': params, 'report_day': args.day,
                          'denominator_hash': denominator.denominator_hash,
                          'denominator_version': denominator.denominator_version,
                          'blocked_competitions': dict(denominator.blocked_competitions),
                          'current_editions': denominator.rows,
                          'denominator_definition_hash': denominator.denominator_definition_hash}, default=str, ensure_ascii=False))
        return 0
    report = json.loads(args.report_json.read_text())
    def ops_clock(value):
        stamp = datetime.fromisoformat(value) if isinstance(value, str) else value
        return utc(stamp.replace(tzinfo=stamp.tzinfo or timezone.utc))
    start, end = day_bounds(args.day)
    if (report.get('measurement_version') != VERSION or ops_clock(report['day_start']) != start
        or ops_clock(report['day_end']) != end or ops_clock(report['as_of']) != utc(args.as_of)):
        raise MeasurementError('daily SQL report version/day/as_of differs from requested evidence')
    report['report_day'] = args.day
    samples = import_site_sample(json.loads(args.samples_json.read_text()), evidence_dir=args.samples_json.parent,
                                 day=args.day, as_of=args.as_of) if args.samples_json else None
    dq_status = 'unknown'
    if args.dq_json and args.dq_state_root:
        parser.error('choose one DQ input')
    if args.dq_json or args.dq_state_root:
        from dags.utils.transfermarkt_daily_dq import VERSION as DQ_VERSION, read_daily_result
        # Select the requested report day even when its freshness cohort matures the next day.
        dq = (json.loads(args.dq_json.read_text()) if args.dq_json
              else read_daily_result(state_root=args.dq_state_root, now=start))
        if (dq.get('day_msk') == args.day and dq.get('version') == DQ_VERSION
            and dq.get('completed') is True and not dq.get('stale')
            and utc(dq['cutoff_at']) <= utc(args.as_of)):
            dq_status = dq.get('status', 'unknown')
    print(json.dumps({'report': report, 'site_sample': samples,
                      'summary': format_daily_summary(report, dq_status=dq_status, site_sample=samples)},
                     default=str, ensure_ascii=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
