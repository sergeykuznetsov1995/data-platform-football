"""Import-light publication and scheduling policy for FBref.

Keep this module free of Airflow, database, and scraper imports so the DAG,
control plane, and tests can share one definition without import side effects.
"""

from __future__ import annotations

import re
from datetime import date, timedelta
from urllib.parse import urlsplit


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
    (*PUBLICATION_REQUIRED_PAGE_KINDS, "standings", "squad")
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

# Parser/document version stamps, centralized in this import-light module so the
# control plane (control/store.py) can read them without pulling the heavy parser
# stack (pandas/pyarrow/bs4 via page_document/typed_bronze/discovery) into its
# import closure.  page_document / typed_bronze / discovery re-export these names;
# see those modules for the per-version rationale.
PAGE_DOCUMENT_VERSION = "fbref-page-document-v6"
TYPED_BRONZE_PARSER_VERSION = "fbref-typed-bronze-v8"
DISCOVERY_PARSER_VERSION = "fbref-discovery-parser-v6"

# Match data tables are written in this order. The per-match availability
# record is published after all of them, including the legacy player table.
MATCH_DATASET_NAMES = (
    "shot_events", "match_events", "lineups", "match_team_stats",
    "match_managers", "match_officials", "match_keeper_stats", "match_meta",
    "match_player_stats",
)
MATCH_TYPED_MANIFESTS = frozenset(f"typed:{name}" for name in MATCH_DATASET_NAMES)

# Discovery classifies every /squads/<id>/ URL as a squad. Use the same path
# rule in the fetch pipeline and when selecting a roster for the acceptance cohort.
SQUAD_NON_ROSTER_PATH = (
    r"/+squads/+[^/]+/+(?:[^/]+/+)*(?:matchlogs|goallogs|history|wages)(?:/|$)"
)


def is_squad_non_roster_url(canonical_url: str) -> bool:
    return re.search(SQUAD_NON_ROSTER_PATH, urlsplit(canonical_url).path) is not None

# Latest documented review of the published season routes in the #945 audit
# (docs/reviews/fbref/2026-07-14-issue-945-production-readiness.md).
# Expiry calls for another evidence check; the navigation link itself proves
# nothing about the availability of the statistical cells.
SEASON_ROUTE_REVIEW_DATES = dict.fromkeys(
    ("passing", "passing_types", "gca", "defense", "possession", "keepersadv"),
    date(2026, 7, 14),
)
UNAVAILABLE_SEASON_STAT_ROUTES = frozenset(SEASON_ROUTE_REVIEW_DATES)
RESTRICTED_DATA_REVIEW_INTERVAL = timedelta(days=30)
EMPTY_MATCH_DATA_REVIEW_INTERVAL = timedelta(days=7)

# Current enrichment stays below the match/schedule drain. Eight hours leave
# one six-hour scheduling phase plus two hours of ordinary execution slack.
CURRENT_ENRICHMENT = {
    "season": ("weekly", 60, 160),
    "season_stats": ("weekly", 55, 160),
    "squad": ("monthly", 50, 664),
}
