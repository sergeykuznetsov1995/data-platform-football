"""
ID Extractors
=============

Functions for extracting player IDs, team IDs, and match URLs
from FBref HTML elements.
"""

import logging
import re
from typing import Dict, Optional

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Pattern for extracting player_id from FBref player links
# Format: /players/{8-char-hash}/Player-Name or /en/players/{8-char-hash}/Player-Name
PLAYER_ID_PATTERN = re.compile(r'/players/([a-f0-9]{8})/')

# Pattern for extracting team_id from FBref squad links
# Format: /squads/{8-char-hash}/Team-Name or /en/squads/{8-char-hash}/Team-Name
TEAM_ID_PATTERN = re.compile(r'/squads/([a-f0-9]{8})/')

# Pattern for extracting manager_id from FBref manager links
# Format: /managers/{8-char-hash}/Manager-Name or /en/managers/{8-char-hash}/Manager-Name
MANAGER_ID_PATTERN = re.compile(r'/managers/([a-f0-9]{8})/')


def extract_player_ids_from_table(
    table: BeautifulSoup,
) -> Dict[int, str]:
    """
    Extract player_id from table rows by parsing player links.

    FBref player links have the format:
    /players/{8-char-hash}/Player-Name
    or
    /en/players/{8-char-hash}/Player-Name

    This function extracts the player_id hash from each row's player link.

    Args:
        table: BeautifulSoup table element

    Returns:
        Dictionary mapping DataFrame row index to player_id (8-char hash).
        The index corresponds to the row position in the resulting DataFrame,
        not the HTML row index (which may include spacer/header rows).
    """
    player_ids = {}

    if table is None:
        return player_ids

    # Process ALL tbody and tfoot sections (pd.read_html includes all)
    # Some FBref tables have multiple <tbody> elements
    # This ensures indices match DataFrame rows correctly
    sections = []

    # Find ALL tbody elements (not just the first one)
    for tbody in table.find_all('tbody'):
        sections.append(tbody)

    # Also include tfoot
    tfoot = table.find('tfoot')
    if tfoot:
        sections.append(tfoot)

    if not sections:
        sections = [table]

    # Use separate counter for data rows (excluding spacers/headers)
    # This ensures indices match DataFrame rows after pd.read_html()
    data_row_idx = 0

    for section in sections:
        for row in section.find_all('tr'):
            # Check row classes
            row_class = row.get('class', [])
            if isinstance(row_class, str):
                row_class = [row_class]

            # pd.read_html SKIPS empty "spacer" rows but INCLUDES "thead" class rows
            # So we should:
            # - Skip spacer rows (don't count them)
            # - Count thead/over_header rows but don't extract player_id from them
            is_spacer = 'spacer' in row_class
            is_inline_header = any(c in row_class for c in ['thead', 'over_header'])

            # Skip spacer rows entirely (pd.read_html also skips them)
            if is_spacer:
                continue

            # For inline headers (thead class inside tbody), count but don't extract
            if not is_inline_header:
                # This is a data row - look for player link
                player_id_found = False

                # Priority 1: th/td with data-stat="player" attribute
                player_cell = row.find(['th', 'td'], attrs={'data-stat': 'player'})
                if player_cell:
                    player_link = player_cell.find('a', href=True)
                    if player_link:
                        href = player_link.get('href', '')
                        match = PLAYER_ID_PATTERN.search(href)
                        if match:
                            player_ids[data_row_idx] = match.group(1)
                            player_id_found = True

                # Priority 2: Any link containing /players/ in the row
                if not player_id_found:
                    for link in row.find_all('a', href=True):
                        href = link.get('href', '')
                        match = PLAYER_ID_PATTERN.search(href)
                        if match:
                            player_ids[data_row_idx] = match.group(1)
                            break

            # Increment counter for all non-spacer rows (to match pd.read_html)
            data_row_idx += 1

    if player_ids:
        logger.debug(f"Extracted {len(player_ids)} player IDs from {data_row_idx} data rows")

    return player_ids


def extract_team_ids_from_table(
    table: BeautifulSoup,
) -> Dict[int, str]:
    """
    Extract team_id from table rows by parsing squad/team links.

    FBref squad links have the format:
    /squads/{8-char-hash}/Team-Name
    or
    /en/squads/{8-char-hash}/Team-Name

    This function extracts the team_id hash from each row's squad link.

    Args:
        table: BeautifulSoup table element

    Returns:
        Dictionary mapping DataFrame row index to team_id (8-char hash).
        The index corresponds to the row position in the resulting DataFrame,
        not the HTML row index (which may include spacer/header rows).
    """
    team_ids = {}

    if table is None:
        return team_ids

    # Process ALL tbody and tfoot sections (pd.read_html includes all)
    sections = []

    for tbody in table.find_all('tbody'):
        sections.append(tbody)

    tfoot = table.find('tfoot')
    if tfoot:
        sections.append(tfoot)

    if not sections:
        sections = [table]

    data_row_idx = 0

    for section in sections:
        for row in section.find_all('tr'):
            row_class = row.get('class', [])
            if isinstance(row_class, str):
                row_class = [row_class]

            is_spacer = 'spacer' in row_class
            is_inline_header = any(c in row_class for c in ['thead', 'over_header'])

            if is_spacer:
                continue

            if not is_inline_header:
                team_id_found = False

                # Priority 1: th/td with data-stat="team" or "squad"
                for stat_name in ['team', 'squad']:
                    team_cell = row.find(['th', 'td'], attrs={'data-stat': stat_name})
                    if team_cell:
                        team_link = team_cell.find('a', href=True)
                        if team_link:
                            href = team_link.get('href', '')
                            match = TEAM_ID_PATTERN.search(href)
                            if match:
                                team_ids[data_row_idx] = match.group(1)
                                team_id_found = True
                                break

                # Priority 2: Any link containing /squads/ in the row
                if not team_id_found:
                    for link in row.find_all('a', href=True):
                        href = link.get('href', '')
                        match = TEAM_ID_PATTERN.search(href)
                        if match:
                            team_ids[data_row_idx] = match.group(1)
                            break

            data_row_idx += 1

    if team_ids:
        logger.debug(f"Extracted {len(team_ids)} team IDs from {data_row_idx} data rows")

    return team_ids


def extract_match_urls_from_schedule(
    soup: BeautifulSoup,
    comment_tables: Optional[Dict[str, BeautifulSoup]] = None,
    season_str: str = None,
    comp_id: str = None,
) -> Dict[int, str]:
    """
    Extract match URLs from schedule table HTML.

    FBref schedule tables have links like:
    <td><a href="/en/matches/abc123/...">Match Report</a></td>

    pandas.read_html() extracts text, not href URLs. This function
    extracts the href URLs to enable match_id extraction.

    Args:
        soup: BeautifulSoup object
        comment_tables: Tables extracted from comments
        season_str: Season string (e.g., "2024-2025")
        comp_id: Competition ID

    Returns:
        Dictionary mapping row index to match URL
    """
    match_urls = {}

    # Find schedule table
    table_ids = [
        'sched_all',
        'sched_ks_all',
    ]
    if season_str and comp_id:
        table_ids.insert(0, f'sched_{season_str}_{comp_id}_1')

    table = None
    for table_id in table_ids:
        table = soup.find('table', id=table_id)
        if table is None and comment_tables:
            table = comment_tables.get(table_id)
        if table is not None:
            break

    # Try finding any table with 'sched' in ID
    if table is None:
        all_tables = soup.find_all(
            'table',
            id=lambda x: x and 'sched' in x.lower()
        )
        if all_tables:
            table = all_tables[0]

    if table is None:
        logger.warning("No schedule table found for match URL extraction")
        return match_urls

    # Extract URLs from table rows
    tbody = table.find('tbody')
    if tbody is None:
        tbody = table

    # Match ID pattern: 8-character hex hash after /matches/
    match_id_pattern = re.compile(r'/matches/([a-f0-9]{8})/')

    for idx, row in enumerate(tbody.find_all('tr')):
        # Look for "Match Report" links (not date links like /matches/2024-08-16)
        for link in row.find_all('a', href=True):
            href = link['href']
            link_text = link.get_text(strip=True)

            # Priority 1: Link with "Match Report" text
            if 'Match Report' in link_text and '/matches/' in href:
                match_urls[idx] = href
                break

            # Priority 2: Link with match ID hash (not date)
            if match_id_pattern.search(href):
                match_urls[idx] = href
                break

    logger.debug(f"Extracted {len(match_urls)} match URLs from schedule")
    return match_urls
