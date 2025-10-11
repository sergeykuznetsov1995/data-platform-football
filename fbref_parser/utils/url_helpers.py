"""
URL manipulation utilities for FBref.com

This module provides functions for:
- Extracting player names from URLs
- Building player URLs
- Converting URLs to all_comps format
"""

import re


def extract_player_name_from_url(url: str) -> str:
    """
    Extract player name from FBref URL

    Extracts the player name from URL patterns like:
    .../William-Saliba-Stats---All-Competitions

    Args:
        url: FBref player URL

    Returns:
        Player name with spaces (e.g., "William Saliba")
    """
    match = re.search(r'/([^/]+)-Stats', url)
    if match:
        return match.group(1).replace('-', ' ')
    return "unknown_player"


def build_player_url(player_id: str, player_name: str = None) -> str:
    """
    Build URL for player page on FBref

    Creates all_comps URL format:
    https://fbref.com/en/players/{player_id}/all_comps/{Player-Name}-Stats---All-Competitions

    Args:
        player_id: FBref player ID
        player_name: Player name (optional, defaults to "Player")

    Returns:
        Complete FBref player URL
    """
    if not player_name:
        player_name = "Player"
    url_name = player_name.replace(' ', '-')
    return f"https://fbref.com/en/players/{player_id}/all_comps/{url_name}-Stats---All-Competitions"


def convert_to_all_comps_url(url: str, player_name: str = None) -> str:
    """
    Convert regular FBref URL to all_comps format

    Converts URLs like:
    .../players/{id}/2023-2024/...
    to:
    .../players/{id}/all_comps/...

    Args:
        url: Original FBref URL
        player_name: Optional player name for URL reconstruction

    Returns:
        URL in all_comps format
    """
    # If already in all_comps format, return as is
    if '/all_comps/' in url:
        return url

    # Try to convert year-specific URL to all_comps
    if '/players/' in url:
        # Replace season-specific part with all_comps
        url = re.sub(r'(/players/[^/]+/)\d{4}-\d{4}/', r'\1all_comps/', url)
        url = re.sub(r'/[^/]*-Stats$', r'Stats---All-Competitions', url)

        # If replacement didn't work, rebuild URL
        if not url.endswith('Stats---All-Competitions'):
            player_id = url.split('/players/')[1].split('/')[0]
            if player_name:
                normalized_name = player_name.replace(' ', '-')
                url = f"https://fbref.com/en/players/{player_id}/all_comps/{normalized_name}-Stats---All-Competitions"

    return url
