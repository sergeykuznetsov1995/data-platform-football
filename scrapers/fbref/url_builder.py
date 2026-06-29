"""
FBref URL Builder
=================

Functions for building FBref URLs.
"""

from scrapers.fbref.constants import BASE_URL, LEAGUE_IDS


def format_season(season: int) -> str:
    """
    Format season year to FBref format.

    FBref uses format like '2023-2024' for full seasons.

    Args:
        season: Season start year (e.g., 2023 for 2023-2024 season)

    Returns:
        Formatted season string
    """
    return f"{season}-{season + 1}"


def get_schedule_url(league: str, season: int) -> str:
    """
    Build URL for schedule/fixtures page.

    Args:
        league: League name
        season: Season year

    Returns:
        Full URL to schedule page

    Raises:
        ValueError: If league is not supported
    """
    league_info = LEAGUE_IDS.get(league)
    if not league_info:
        raise ValueError(f"Unknown league: {league}")

    comp_id = league_info['comp_id']
    slug = league_info['slug']
    season_str = format_season(season)

    return (
        f"{BASE_URL}/en/comps/{comp_id}/{season_str}/schedule/"
        f"{season_str}-{slug}-Scores-and-Fixtures"
    )


def get_stats_url(
    league: str,
    season: int,
    stat_type: str,
    for_squads: bool = False
) -> str:
    """
    Build URL for statistics page.

    Args:
        league: League name
        season: Season year
        stat_type: Type of statistics
        for_squads: True for team stats, False for player stats

    Returns:
        Full URL to stats page

    Raises:
        ValueError: If league is not supported
    """
    league_info = LEAGUE_IDS.get(league)
    if not league_info:
        raise ValueError(f"Unknown league: {league}")

    comp_id = league_info['comp_id']
    slug = league_info['slug']
    season_str = format_season(season)

    # Map stat_type to FBref URL path (FBref uses different naming)
    url_stat_type_mapping = {
        'keeper': 'keepers',           # FBref uses plural 'keepers'
        'playingtime': 'playingtime',  # FBref uses 'playingtime' (no underscore) for player URL
    }
    url_stat_type = url_stat_type_mapping.get(stat_type, stat_type)

    # For playingtime squad stats, FBref uses 'playing_time' with underscore
    if stat_type == 'playingtime' and for_squads:
        url_stat_type = 'playing_time'

    # Both squad and player stats use the same URL structure
    return (
        f"{BASE_URL}/en/comps/{comp_id}/{season_str}/{url_stat_type}/"
        f"{season_str}-{slug}-Stats"
    )
