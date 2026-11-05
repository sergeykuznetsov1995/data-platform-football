"""Night-only admission for FotMob Silver's production DB-API cursor.

No writes outside this cursor in the DAG. This is conservative admission,
not a cluster reservation: foreign queries may start after the probe. Trino's
timeout bounds execution, but cannot guarantee an exact wall-clock commit
deadline. A timeout at commit requires reader-state readback before retry.
"""
from __future__ import annotations

from datetime import datetime, time, timezone
from math import floor
from zoneinfo import ZoneInfo

MSK = ZoneInfo('Europe/Moscow')
START = time(0, 30)
END = time(2, 45)
LOAD_SQL = """SELECT count(*) FROM system.runtime.queries
WHERE state = 'RUNNING'
AND strpos(query, 'fotmob_silver_admission_probe') = 0
/* fotmob_silver_admission_probe */"""


class AdmissionClosed(RuntimeError):
    """Stop this run without publishing; resume the saved build next night."""


def remaining_seconds(now: datetime) -> int:
    if now.tzinfo is None:
        raise ValueError('admission requires a timezone-aware clock')
    local = now.astimezone(MSK)
    if not START <= local.time() < END:
        raise AdmissionClosed('FotMob Silver window is 00:30–02:45 MSK')
    remaining = floor((datetime.combine(local.date(), END, MSK) - local).total_seconds())
    if remaining < 1:
        raise AdmissionClosed('FotMob Silver window has less than one second left')
    return min(1800, remaining)


class AdmittedCursor:
    """Guard every statement, including slot bootstrap, barrier and publication.

    Busy/unknown load fails closed rather than holding an Airflow worker. The
    caller must close the connection, preserve checkpoints, and leave schedule
    disabled until production acceptance is authorized.
    """

    def __init__(self, cursor, *, now=None):
        self._cursor = cursor
        self._now = now or (lambda: datetime.now(timezone.utc))

    def _timeout(self):
        # Leave five seconds for SET/transport overhead. Never admit work with
        # a now-stale timeout after a slow SET response.
        seconds = remaining_seconds(self._now()) - 5
        if seconds < 1:
            raise AdmissionClosed('FotMob Silver has no query budget after transport reserve')
        self._cursor.execute(f"SET SESSION query_max_run_time = '{seconds}s'")
        self._cursor.fetchall()
        if seconds > remaining_seconds(self._now()):
            raise AdmissionClosed('FotMob Silver timeout setup consumed its transport reserve')
        return seconds

    def execute(self, sql, *args, **kwargs):
        self._timeout()
        self._cursor.execute(LOAD_SQL)
        rows = self._cursor.fetchall()
        if (len(rows) != 1 or len(rows[0]) != 1
                or type(rows[0][0]) is not int or rows[0][0] < 0):
            raise AdmissionClosed('FotMob Silver load probe returned no reliable count')
        if rows[0][0] > 5:
            raise AdmissionClosed(f'FotMob Silver busy: {rows[0][0]} foreign RUNNING queries')
        # Probe time counts against the window. Refresh the server-side bound,
        # then recheck after SET before admitting the actual statement.
        seconds = self._timeout()
        if seconds > remaining_seconds(self._now()):
            raise AdmissionClosed('FotMob Silver query budget expired before submission')
        return self._cursor.execute(sql, *args, **kwargs)

    def fetchall(self):
        return self._cursor.fetchall()

