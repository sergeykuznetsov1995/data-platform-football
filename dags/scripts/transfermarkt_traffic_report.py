#!/usr/bin/env python3
"""Transfermarkt paid traffic for one Moscow day and history step-1 status (#1404).

Reads the gateway ledger (files), signal-lane receipts (files) and ops tables
(SELECT only); no site requests or writes. In the contour:

    docker exec transfermarkt-airflow-scheduler python /opt/airflow/dags/scripts/transfermarkt_traffic_report.py --day 2026-10-30

On the host: ``--ledger-dir <runtime>/gateway-state --no-trino``.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / 'dags') not in sys.path:
    sys.path.append(str(ROOT / 'dags'))


def _read(cur, sql, params):
    cur.execute(sql, params)
    columns = [item[0] for item in (cur.description or ())]
    return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]


def execute_trino(run_ids, campaign):
    """Read each ops source and the history blocks independently.

    Returns (ops rows, {source: ok|not_available}, editions block); successful
    reads are kept when another fails. Errors never leak connection details.
    """
    from dags.utils import transfermarkt_traffic_report as traffic
    from dags.utils import transfermarkt_native_v2 as native

    rows, sources = [], {'events_v1': 'not_available', 'proxy_traffic_runs': 'not_available'}
    editions = {'status': 'not_available', 'reason': 'ошибка чтения журнала истории'}
    try:
        connection = native.connect()
    except Exception:
        return rows, sources, {'status': 'not_available', 'reason': 'нет соединения с Trino'}
    try:
        cur = connection.cursor()
        try:
            for source, build in (('events_v1', traffic.build_ops_events_sql),
                                  ('proxy_traffic_runs', traffic.build_ops_runs_sql)):
                try:
                    rows.extend(_read(cur, *build(run_ids)) if run_ids else [])
                    sources[source] = 'ok'
                except Exception:
                    pass
            try:
                campaigns = _read(cur, *traffic.build_history_campaigns_sql(campaign))
                scopes = _read(cur, *traffic.build_history_editions_sql(campaign)) if campaigns else []
                editions = traffic.summarize_editions(scopes, campaigns, campaign)
            except Exception:
                pass
        finally:
            cur.close()
    finally:
        connection.close()
    return rows, sources, editions


def main(argv=None) -> int:
    from dags.utils import transfermarkt_traffic_report as traffic
    from dags.utils.transfermarkt_daily_measure import day_bounds, utc

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--day', required=True, help='YYYY-MM-DD, Moscow calendar day')
    parser.add_argument('--ledger-dir', type=Path, default=Path('/opt/airflow/gateway-state'))
    parser.add_argument('--current-state-dir', type=Path, default=Path(os.environ.get(
        'TM_CURRENT_STATE_DIR', '/opt/airflow/logs/transfermarkt-native-v2/current')))
    parser.add_argument('--as-of', help='ISO8601 with timezone; default now UTC. Runs with bytes '
                        'younger than the lease TTL at as_of are not reconciled')
    parser.add_argument('--campaign', help='history campaign_id; default: every campaign')
    parser.add_argument('--no-trino', action='store_true', help='ledger and receipts only')
    parser.add_argument('--json', type=Path, help='also write the full report to this file')
    args = parser.parse_args(argv)
    start, end = day_bounds(args.day)
    as_of = utc(args.as_of) if args.as_of else datetime.now(timezone.utc)
    try:
        ledger = traffic.read_ledger(args.ledger_dir, start, end, as_of)
    except (OSError, traffic.TrafficReportError):
        print(json.dumps({'report': None, 'status': 'not_measured',
                          'summary': f'Transfermarkt трафик {args.day} МСК: не измерено — ошибка чтения леджера шлюза'},
                         ensure_ascii=False))
        return 1
    run_ids = [run_id for run_id in ledger['runs'] if run_id]
    receipts = traffic.read_current_receipts(args.current_state_dir, run_ids)
    if args.no_trino:
        ops = []
        sources = {'events_v1': 'not_available', 'proxy_traffic_runs': 'not_available'}
        editions = {'status': 'not_available', 'reason': 'Trino не читали (--no-trino)'}
    else:
        ops, sources, editions = execute_trino(run_ids, args.campaign)
    sources['current_receipts'] = receipts['status']
    report = {'report_version': traffic.VERSION, 'report_day': args.day,
              'day_start': start.isoformat(), 'day_end': end.isoformat(), 'as_of': as_of.isoformat(),
              'ledger': ledger,
              'ops_sources': sources,
              'current_receipts': {key: value for key, value in receipts.items() if key != 'rows'},
              **traffic.merge_ops(ledger, ops, receipts, sources), 'history_editions': editions}
    summary = traffic.format_traffic_summary(report) + '\n' + traffic.format_editions_summary(report)
    payload = json.dumps({'report': report, 'summary': summary}, default=str, ensure_ascii=False)
    if args.json:
        args.json.write_text(payload + '\n', encoding='utf-8')
    print(payload)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
