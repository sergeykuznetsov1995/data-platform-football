"""When a finished SofaScore match is due in Bronze (#1359).

One rule for the refresh queue and for the milestone-1 meter
(``docs/operations/sofascore-production.md``, "Определения"): a match ENDS at
the source's ``changes_change_timestamp`` when that lies within
``[start + 45 min, start + 4 h]``, otherwise at ``start + 2 h``; its DEADLINE is
the end + 24 h.  The queue has to optimise exactly what is measured, so the
SQL text lives here once and every query formats it in.
"""

from __future__ import annotations

from typing import Optional

END_MIN_AFTER_START_S = 45 * 60
END_MAX_AFTER_START_S = 4 * 3600
END_FALLBACK_AFTER_START_S = 2 * 3600
DEADLINE_AFTER_END_S = 24 * 3600

# ``{start}`` / ``{change}`` are column expressions (epoch seconds; the schedule
# stores both as double).  A NULL start gives a NULL deadline: no evidence, no
# deadline.
MATCH_DEADLINE_SQL = (
    "(CASE WHEN {change} BETWEEN {start} + " + str(END_MIN_AFTER_START_S)
    + " AND {start} + " + str(END_MAX_AFTER_START_S)
    + " THEN {change} ELSE {start} + " + str(END_FALLBACK_AFTER_START_S)
    + " END) + " + str(DEADLINE_AFTER_END_S)
)


def match_deadline_sql(
    start: str = "start_timestamp", change: str = "changes_change_timestamp"
) -> str:
    """The deadline expression over the given column expressions."""

    return MATCH_DEADLINE_SQL.format(start=start, change=change)


def match_deadline(start: Optional[float], change: Optional[float]) -> Optional[int]:
    """The same rule in Python (the meter's own code path and the tests)."""

    if start is None:
        return None
    start = int(start)
    if change is not None and (
        start + END_MIN_AFTER_START_S <= int(change) <= start + END_MAX_AFTER_START_S
    ):
        end = int(change)
    else:
        end = start + END_FALLBACK_AFTER_START_S
    return end + DEADLINE_AFTER_END_S


__all__ = [
    "DEADLINE_AFTER_END_S",
    "MATCH_DEADLINE_SQL",
    "match_deadline",
    "match_deadline_sql",
]
