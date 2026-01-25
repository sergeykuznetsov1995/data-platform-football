"""PyArrow and Iceberg schemas for football data."""

from scrapers.schemas.common import METADATA_FIELDS
from scrapers.schemas.fbref import (
    FBREF_SCHEDULE_SCHEMA,
    FBREF_PLAYER_STATS_SCHEMA,
    FBREF_TEAM_STATS_SCHEMA,
)
from scrapers.schemas.understat import (
    UNDERSTAT_SHOTS_SCHEMA,
    UNDERSTAT_PLAYERS_SCHEMA,
)
from scrapers.schemas.whoscored import WHOSCORED_EVENTS_SPADL_SCHEMA

__all__ = [
    'METADATA_FIELDS',
    'FBREF_SCHEDULE_SCHEMA',
    'FBREF_PLAYER_STATS_SCHEMA',
    'FBREF_TEAM_STATS_SCHEMA',
    'UNDERSTAT_SHOTS_SCHEMA',
    'UNDERSTAT_PLAYERS_SCHEMA',
    'WHOSCORED_EVENTS_SPADL_SCHEMA',
]
