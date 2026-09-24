"""Daily Understat "played -> Bronze within 24 h" meter (#1429)."""

from __future__ import annotations

import re
from decimal import Decimal

import pytest

from scrapers.understat.criterion import (
    COMPLETENESS_SQL,
    DAILY_CRITERION_SQL,
    DayResult,
    pct,
    render_daily_criterion_sql,
    summarize_days,
)

pytestmark = pytest.mark.unit


def test_days_without_deadlines_are_neutral_and_do_not_break_the_streak():
    days = [
        DayResult("2026-10-10", due=0, ok=0),
        DayResult("2026-10-11", due=10, ok=10),
        DayResult("2026-10-12", due=0, ok=0),
        DayResult("2026-10-13", due=8, ok=8),
        DayResult("2026-10-14", due=0, ok=0),
        DayResult("2026-10-15", due=12, ok=12),
        DayResult("2026-10-16", due=0, ok=0),
    ]
    assert summarize_days(days) == 3

    missed = days + [DayResult("2026-10-09", due=10, ok=9)]
    assert summarize_days(missed) == 3
    assert summarize_days(days + [DayResult("2026-10-17", due=10, ok=9)]) == 0


def test_site_delay_is_subtracted_from_due_and_counted_separately():
    # One match the site had not marked played by its deadline: it is not in
    # due, so the day stays 100 % and keeps the streak.
    day = DayResult("2026-09-13", due=28, ok=28, site_late=1)
    assert day.pct == Decimal("100.0")
    assert summarize_days([day]) == 1

    sql = re.sub(r"\s+", " ", DAILY_CRITERION_SQL)
    assert "count_if(NOT site_late) AS due" in sql
    assert "count_if(site_late) AS site_late" in sql
    assert (
        "covered_hits = 0 AND attempts > 0 AND site_hits = 0 AS site_late" in sql
    )


def test_pct_rounds_half_up_like_the_fotmob_line():
    assert pct(397, 400) == Decimal("99.3")
    assert pct(0, 0) is None
    assert DayResult("2026-10-11", due=400, ok=397).pct == Decimal("99.3")


def test_sql_reads_through_the_manifest_fence_with_a_26_hour_deadline():
    daily = render_daily_criterion_sql("2026-09-13")
    for sql in (daily, COMPLETENESS_SQL):
        assert "ORDER BY completed_at DESC, attempt_id DESC" in sql
        assert "m.status = 'complete'" in sql
        assert re.search(r"\w+\._batch_id = m\.batch_id", sql)
        assert "contract_version = 'understat-bronze-v2'" in sql
    assert "kickoff + INTERVAL '26' HOUR" in daily
    assert "TIMESTAMP '2026-09-13 00:00:00'" in daily
    with pytest.raises(ValueError):
        render_daily_criterion_sql("13.09.2026")
