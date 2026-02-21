"""
Finders
=======

Functions for locating and parsing specific FBref tables:
schedule tables, player/team stats tables, shots, lineups,
match events, and team match statistics.
"""

import logging
import re
from typing import Dict, Optional

import pandas as pd
from bs4 import BeautifulSoup

from scrapers.fbref.parsers.table_parser import (
    parse_table,
    _parse_table_element,
    _table_has_player_header,
)

logger = logging.getLogger(__name__)


def find_schedule_table(
    soup: BeautifulSoup,
    comment_tables: Dict[str, BeautifulSoup],
    season_str: str,
    comp_id: str
) -> Optional[pd.DataFrame]:
    """
    Find and parse schedule table using multiple strategies.

    Args:
        soup: BeautifulSoup object
        comment_tables: Tables extracted from comments
        season_str: Season string (e.g., "2024-2025")
        comp_id: Competition ID

    Returns:
        DataFrame or None
    """
    # Try multiple possible table IDs
    table_ids = [
        'sched_all',
        'sched_ks_all',
        f'sched_{season_str}_{comp_id}_1',
    ]

    df = None
    for table_id in table_ids:
        df = parse_table(soup, table_id, comment_tables)
        if df is not None and not df.empty:
            return df

    # Try finding any table with 'sched' in ID from regular HTML
    all_tables = soup.find_all(
        'table',
        id=lambda x: x and 'sched' in x.lower()
    )
    for table in all_tables:
        table_id = table.get('id')
        df = parse_table(soup, table_id, comment_tables)
        if df is not None and not df.empty:
            logger.debug(f"Found schedule table: {table_id}")
            return df

    # Try parsing any table with 'sched' in ID from comments
    for key in comment_tables.keys():
        if 'sched' in key.lower():
            df = parse_table(soup, key, comment_tables)
            if df is not None and not df.empty:
                return df

    return None


def find_team_stats_table(
    soup: BeautifulSoup,
    comment_tables: Dict[str, BeautifulSoup],
    stat_type: str
) -> Optional[pd.DataFrame]:
    """
    Find and parse team stats table.

    Args:
        soup: BeautifulSoup object
        comment_tables: Tables extracted from comments
        stat_type: Type of statistics

    Returns:
        DataFrame or None
    """
    # Map stat_type to FBref table naming convention
    # FBref uses 'playing_time' with underscore, we use 'playingtime'
    stat_type_mapping = {
        'playingtime': 'playing_time',
    }
    fbref_stat_type = stat_type_mapping.get(stat_type, stat_type)

    # Possible table IDs for squad stats
    table_ids = [
        f'stats_squads_{fbref_stat_type}_for',
        f'stats_squads_{stat_type}_for',  # fallback to original
        f'stats_squads_standard_for',
        f'stats_squads_{fbref_stat_type}',
        f'stats_squads_{stat_type}',
    ]

    df = None
    for table_id in table_ids:
        df = parse_table(soup, table_id, comment_tables, extract_team_ids=True)
        if df is not None and not df.empty:
            return df

    # Try finding table with 'squads' in ID
    for key in comment_tables.keys():
        if 'squads' in key.lower() and stat_type in key.lower():
            df = parse_table(soup, key, comment_tables, extract_team_ids=True)
            if df is not None and not df.empty:
                return df

    return None


def find_player_stats_table(
    soup: BeautifulSoup,
    comment_tables: Dict[str, BeautifulSoup],
    stat_type: str,
    extract_player_ids: bool = True,
) -> Optional[pd.DataFrame]:
    """
    Find and parse player stats table.

    Args:
        soup: BeautifulSoup object
        comment_tables: Tables extracted from comments
        stat_type: Type of statistics
        extract_player_ids: If True, extract player_id from player links

    Returns:
        DataFrame or None
    """
    # Map stat_type to FBref table naming convention
    # FBref uses 'playing_time' with underscore, we use 'playingtime'
    stat_type_mapping = {
        'playingtime': 'playing_time',
    }
    fbref_stat_type = stat_type_mapping.get(stat_type, stat_type)

    # Possible table IDs for player stats
    table_ids = [
        f'stats_{fbref_stat_type}',
        f'stats_{stat_type}',  # fallback to original
        f'stats_standard',
        f'stats_{fbref_stat_type}_all',
        f'stats_{stat_type}_all',
    ]

    df = None
    for table_id in table_ids:
        df = parse_table(
            soup, table_id, comment_tables,
            extract_player_ids=extract_player_ids
        )
        if df is not None and not df.empty:
            logger.debug(f"Found player stats table by ID: {table_id}")
            return df

    # Fallback: Try finding any stats table in DOM
    all_tables = soup.find_all('table')
    logger.debug(f"Fallback: found {len(all_tables)} tables in DOM")

    for table in all_tables:
        table_id = table.get('id', '')

        # Option 1: Table with ID containing 'stats' (excluding squad)
        if table_id and 'stats' in table_id.lower() and 'squad' not in table_id.lower():
            df = parse_table(
                soup, table_id, comment_tables,
                extract_player_ids=extract_player_ids
            )
            if df is not None and not df.empty:
                logger.info(f"Found player stats table by ID pattern: {table_id}")
                return df

        # Option 2: Table without ID, but with 'stats_table' class or similar
        table_class = table.get('class', [])
        if isinstance(table_class, list):
            table_class_str = ' '.join(table_class)
        else:
            table_class_str = str(table_class)

        if 'stats' in table_class_str.lower() and 'squad' not in table_class_str.lower():
            df = _parse_table_element(table, extract_player_ids)
            if df is not None and not df.empty:
                logger.info(f"Found stats table by class: {table_class_str}")
                return df

    # Option 3: Last resort - find table with 'Player' header
    for table in all_tables:
        if _table_has_player_header(table):
            df = _parse_table_element(table, extract_player_ids)
            if df is not None and not df.empty:
                logger.info("Found stats table by 'Player' header")
                return df

    # Option 4: Try comment tables with any stats-related key
    for key in comment_tables.keys():
        if 'stats' in key.lower() and 'squad' not in key.lower():
            df = parse_table(
                soup, key, comment_tables,
                extract_player_ids=extract_player_ids
            )
            if df is not None and not df.empty:
                logger.info(f"Found player stats table in comments: {key}")
                return df

    return None


def parse_shots_table(
    soup: BeautifulSoup,
    comment_tables: Dict[str, BeautifulSoup]
) -> Optional[pd.DataFrame]:
    """
    Parse shots table from match page.

    FBref shot tables contain detailed shot data including:
    - xG (expected goals)
    - Shot type (header, foot, etc.)
    - Shot outcome (goal, saved, blocked, etc.)
    - Distance and body part
    - Minute of shot

    Args:
        soup: BeautifulSoup object of match page
        comment_tables: Tables extracted from comments

    Returns:
        DataFrame with shot events or None
    """
    # Possible table IDs for shots
    table_ids = [
        'shots_all',
        'shots_both',
        'shots',
    ]

    for table_id in table_ids:
        df = parse_table(soup, table_id, comment_tables)
        if df is not None and not df.empty:
            logger.debug(f"Found shots table: {table_id}")
            return df

    # Try finding any table with 'shots' in ID
    for table in soup.find_all('table'):
        table_id = table.get('id', '')
        if 'shot' in table_id.lower():
            df = parse_table(soup, table_id, comment_tables)
            if df is not None and not df.empty:
                return df

    # Check comment tables
    for key in comment_tables.keys():
        if 'shot' in key.lower():
            df = parse_table(soup, key, comment_tables)
            if df is not None and not df.empty:
                return df

    return None


def parse_lineup_table(
    soup: BeautifulSoup,
    team_id: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Parse lineup information from match page.

    Args:
        soup: BeautifulSoup object of match page
        team_id: Optional team ID to filter specific team

    Returns:
        DataFrame with lineup data or None
    """
    lineups = []

    # Find lineup divs - they're usually in divs with class 'lineup'
    lineup_divs = soup.find_all('div', class_=lambda x: x and 'lineup' in str(x).lower())

    if not lineup_divs:
        # Try alternative: find tables with player names in lineup section
        lineup_divs = soup.find_all('div', id=lambda x: x and 'lineup' in str(x).lower())

    for div in lineup_divs:
        # Extract team name
        team_header = div.find(['h2', 'h3', 'th'])
        team_name = team_header.get_text(strip=True) if team_header else 'Unknown'

        # Find all player entries
        players = div.find_all('a', href=lambda x: x and '/players/' in str(x))

        for idx, player in enumerate(players):
            player_link = player.get('href', '')
            player_name = player.get_text(strip=True)

            # Try to get player ID from link
            player_id = None
            if '/players/' in player_link:
                parts = player_link.split('/players/')[-1].split('/')
                if parts:
                    player_id = parts[0]

            # Determine if starter (first 11) or substitute
            is_starter = idx < 11

            # Try to find position and number
            parent = player.find_parent(['tr', 'div', 'li'])
            position = ''
            number = ''

            if parent:
                # Look for position text
                pos_elem = parent.find(
                    string=lambda x: x and any(
                        p in str(x).upper() for p in ['GK', 'DF', 'MF', 'FW', 'SUB']
                    )
                )
                if pos_elem:
                    position = pos_elem.strip()

                # Look for number
                num_elem = parent.find(
                    string=lambda x: x and str(x).strip().isdigit()
                )
                if num_elem:
                    number = num_elem.strip()

            lineups.append({
                'team': team_name,
                'player': player_name,
                'player_id': player_id,
                'is_starter': is_starter,
                'position': position,
                'number': number,
            })

    if lineups:
        return pd.DataFrame(lineups)

    return None


def parse_events_from_scorebox(soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    """
    Parse match events (goals, cards, substitutions) from scorebox.

    FBref shows key events in the scorebox area of match pages.

    Args:
        soup: BeautifulSoup object of match page

    Returns:
        DataFrame with match events or None
    """
    events = []

    # Find scorebox - contains goals, cards, etc.
    scorebox = soup.find('div', class_='scorebox')
    if not scorebox:
        scorebox = soup.find('div', id='scorebox')

    if not scorebox:
        logger.debug("No scorebox found on page")
        return None

    # Find all event divs within scorebox
    # Goals usually have class 'event' or similar
    event_divs = scorebox.find_all('div', class_=lambda x: x and 'event' in str(x).lower())

    if not event_divs:
        # Try finding by content patterns
        all_divs = scorebox.find_all('div')
        for div in all_divs:
            text = div.get_text(strip=True)
            # Look for minute patterns like "45'" or "90+2'"
            if re.search(r"\d+['′]", text):
                event_divs.append(div)

    for div in event_divs:
        text = div.get_text(strip=True)

        # Extract minute
        minute_match = re.search(r"(\d+)['′](\+\d+)?", text)
        minute = minute_match.group(0) if minute_match else ''

        # Determine event type
        event_type = 'unknown'
        if any(icon in str(div) for icon in ['goal', 'soccer', 'ball']):
            event_type = 'goal'
        elif 'yellow' in str(div).lower() or '\U0001f7e8' in text:
            event_type = 'yellow_card'
        elif 'red' in str(div).lower() or '\U0001f7e5' in text:
            event_type = 'red_card'
        elif 'sub' in str(div).lower() or '\U0001f503' in text:
            event_type = 'substitution'
        elif 'pen' in text.lower():
            event_type = 'penalty'
        elif 'og' in text.lower() or 'own goal' in text.lower():
            event_type = 'own_goal'

        # Find player link
        player_link = div.find('a', href=lambda x: x and '/players/' in str(x))
        player_name = player_link.get_text(strip=True) if player_link else ''
        player_id = None
        if player_link:
            href = player_link.get('href', '')
            if '/players/' in href:
                player_id = href.split('/players/')[-1].split('/')[0]

        # Find team (from parent structure)
        team = ''
        parent = div.find_parent(['div'])
        if parent:
            team_link = parent.find('a', href=lambda x: x and '/squads/' in str(x))
            if team_link:
                team = team_link.get_text(strip=True)

        if minute or player_name or event_type != 'unknown':
            events.append({
                'minute': minute,
                'event_type': event_type,
                'player': player_name,
                'player_id': player_id,
                'team': team,
                'description': text[:200],  # Limit description length
            })

    if events:
        return pd.DataFrame(events)

    return None


def parse_team_match_stats_table(
    soup: BeautifulSoup,
    comment_tables: Dict[str, BeautifulSoup],
    team_id: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Parse team-level match statistics table.

    Args:
        soup: BeautifulSoup object
        comment_tables: Tables extracted from comments
        team_id: Optional team ID to filter

    Returns:
        DataFrame with team match stats or None
    """
    # Look for team stats summary tables
    table_ids = [
        'team_stats',
        'team_stats_extra',
    ]

    for table_id in table_ids:
        df = parse_table(soup, table_id, comment_tables)
        if df is not None and not df.empty:
            return df

    # Try finding tables with team stats patterns
    for table in soup.find_all('table'):
        table_id = table.get('id', '')
        # Look for summary tables that aren't player stats
        if ('summary' in table_id.lower() or 'team' in table_id.lower()) \
           and 'player' not in table_id.lower():
            df = parse_table(soup, table_id, comment_tables)
            if df is not None and not df.empty:
                return df

    return None
