"""Import-light publication and scheduling policy for FBref.

Keep this module free of Airflow, database, and scraper imports so the DAG,
control plane, and tests can share one definition without import side effects.
"""

from __future__ import annotations


PUBLICATION_REQUIRED_PAGE_KINDS = (
    "competition_index",
    "competition",
    "season",
    "season_stats",
    "schedule",
    "match",
)

# Standings are publication-critical when FBref exposes them, but they are not
# globally required because some competition layouts do not publish a distinct
# standings page/table.
PUBLICATION_FRESHNESS_PAGE_KINDS = frozenset(
    (*PUBLICATION_REQUIRED_PAGE_KINDS, "standings")
)

DISCOVERY_SPINE_PAGE_KINDS = (
    "competition",
    "season",
    "schedule",
)

OTHER_PUBLICATION_CRITICAL_PAGE_KINDS = (
    "season_stats",
    "standings",
)
