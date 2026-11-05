"""Read the ESPN history line through the installed ESPN client's credentials.

The host has no database credentials. Only fixed history-report SELECTs run in
an existing container; no parser, DDL, message sender or generic SQL endpoint.
"""
from __future__ import annotations

from datetime import date
import subprocess
import sys

REMOTE_CODE = r'''
import sys
import warnings
warnings.filterwarnings('ignore', category=Warning, module='urllib3')
from scrapers.espn.history_report import (
    render_queue_sql, render_journal_sql, render_capability_sql,
    render_history_line, empty_defects,
)
from scrapers.espn.trino_manager import EspnTrinoTableManager

day = sys.argv[1]
unknown = f'• ESPN история {day[8:10]}.{day[5:7]}: не посчитано ⚠️'
try:
    client = EspnTrinoTableManager()
    queue = client.execute_query(render_queue_sql(day))
    journal = client.execute_query(render_journal_sql(day))
    if len(queue) != 1 or len(queue[0]) != 7 or len(journal) != 1 or len(journal[0]) != 2:
        raise ValueError('invalid report shape')
    # Never interpret null, negative or fractional counters as a healthy day.
    for value in list(queue[0]) + list(journal[0]):
        if value is None or int(value) != value or value < 0:
            raise ValueError('invalid counter')
    try:
        capability = client.execute_query(render_capability_sql())
        if any(len(row) != 8 for row in capability):
            raise ValueError('invalid capability shape')
        defects = empty_defects(capability)
    except Exception:
        defects = None
    if queue[0][0] == 0 and queue[0][3] == 0 and queue[0][6] > 0:
        line = (f'• ESPN история {day[8:10]}.{day[5:7]}: очередь пуста, '
                f'прогонов с ошибкой {int(queue[0][6])} ‼️')
    else:
        line = render_history_line(day, queue[0], journal[0], defects)
    if defects is None:
        line = line.replace(' ✅', '') + '; пустые части не посчитаны ⚠️'
    print(line)
except Exception:
    print(unknown)
'''


def history_line(day: str) -> str:
    """Return a line or explicit unknown; never send it anywhere."""
    parsed = date.fromisoformat(day)
    if parsed.isoformat() != day:
        raise ValueError('canonical YYYY-MM-DD required')
    prefix = f'• ESPN история {parsed:%d.%m}:'
    unknown = prefix + ' не посчитано ⚠️'
    command = [
        'docker', 'exec', '-w', '/tmp', '-e', 'PYTHONDONTWRITEBYTECODE=1',
        'espn-live-airflow-scheduler-1', 'python', '-B', '-c', REMOTE_CODE, day,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=240)
    except (OSError, subprocess.TimeoutExpired):
        return unknown
    line = result.stdout.strip()
    if result.returncode or '\n' in line or not line.startswith(prefix + ' '):
        return unknown
    return line


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('usage: espn_history_host.py YYYY-MM-DD')
    print(history_line(sys.argv[1]))
