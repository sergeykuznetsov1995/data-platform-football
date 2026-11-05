"""Drain older Trino readers before slot reuse or publication.

Trino 482 Iceberg pins snapshots per physical table, not per bundle. A
state-selected view therefore also needs a barrier after the last build
commit and before the state CAS, and before reusing the former active slot.
The caller must use autocommit, consume its own writes, and serialize writers.
This conservative barrier waits for ALL older transactions, including idle
explicit transactions; it never cancels another query. It requires one
coordinator and access to system.runtime.transactions. A failure blocks the
caller's next mutation. Timestamp ordering assumes the coordinator clock does
not move backwards during the barrier.
"""
from __future__ import annotations

import datetime as dt
import time


def drain_readers(cur, *, timeout_seconds: float = 300, poll_seconds: float = 1) -> None:
    """Wait for transactions predating a post-commit watermark to disappear.

    Unlike runtime.queries, runtime.transactions in Trino 482 does not filter
    other users' transactions. Registration precedes analysis/table-handle reads.
    Newer transactions need not drain: at entry, the relevant slots are stable.
    """
    if timeout_seconds < 0 or poll_seconds <= 0:
        raise ValueError('invalid reader barrier timing')
    cur.execute('SELECT count(*) FROM system.runtime.nodes WHERE coordinator')
    rows = cur.fetchall()
    if len(rows) != 1 or len(rows[0]) != 1 or rows[0][0] != 1:
        raise RuntimeError('reader barrier requires exactly one coordinator')
    cur.execute('SELECT to_iso8601(current_timestamp)')
    rows = cur.fetchall()
    if len(rows) != 1 or len(rows[0]) != 1 or not isinstance(rows[0][0], str):
        raise RuntimeError('reader barrier watermark missing')
    cutoff = rows[0][0]
    parsed = dt.datetime.fromisoformat(cutoff.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('reader barrier watermark requires a timezone')
    literal = cutoff.replace("'", "''")
    sql = (
        'SELECT transaction_id FROM system.runtime.transactions\n'
        f"WHERE create_time <= from_iso8601_timestamp('{literal}')"
    )
    deadline = time.monotonic() + timeout_seconds
    # System table timestamps have millisecond precision. The observation must
    # start later than its watermark, so it does not count its own transaction.
    time.sleep(0.01)
    while True:
        cur.execute(sql)
        blockers = cur.fetchall()
        if not blockers:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f'reader barrier: {len(blockers)} older transaction(s) still active')
        time.sleep(min(poll_seconds, max(0, deadline - time.monotonic())))
