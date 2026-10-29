"""Adapter for the host morning summary; no sending or ingestion side effects."""
from datetime import datetime, timezone

from scrapers.clubelo.dq import format_summary, read_report


def summary_lines(trino, *, today=None, max_age_days=3):
    """Use the host's existing read-only Trino client and UTC reference date."""
    today = today if today is not None else datetime.now(timezone.utc).date()
    return format_summary(read_report(trino, today=today, max_age_days=max_age_days))
