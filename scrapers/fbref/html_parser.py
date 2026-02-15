"""
FBref HTML Parser
=================

Functions for parsing FBref HTML, including extraction of tables
hidden in HTML comments.

Memory Optimization Notes:
- BeautifulSoup objects from comments are decomposed after extraction
- Intermediate soup objects are cleared to free memory
"""

import gc
import logging
import re
from io import StringIO
from typing import Any, Dict, Optional

import pandas as pd
from bs4 import BeautifulSoup, Comment

logger = logging.getLogger(__name__)

# Pattern for extracting player_id from FBref player links
# Format: /players/{8-char-hash}/Player-Name or /en/players/{8-char-hash}/Player-Name
PLAYER_ID_PATTERN = re.compile(r'/players/([a-f0-9]{8})/')

# Pattern for extracting team_id from FBref squad links
# Format: /squads/{8-char-hash}/Team-Name or /en/squads/{8-char-hash}/Team-Name
TEAM_ID_PATTERN = re.compile(r'/squads/([a-f0-9]{8})/')


# Prefixes added by FBref MultiIndex headers that should be normalized
# Generate Unnamed: X_level_0_ prefixes for indices 0-50 to cover all tables
MULTIINDEX_PREFIXES = [
    f'Unnamed: {i}_level_0_' for i in range(100)
] + [
    # Standard stat categories
    'Standard_',
    'Shooting_',
    'Passing_',
    'Pass Types_',
    'Goal and Shot Creation_',
    'Defensive Actions_',
    'Possession_',
    'Playing Time_',
    'Miscellaneous Stats_',
    'Miscellaneous_',
    'Misc_',
    # Performance metrics
    'Performance_',
    'Expected_',
    'Progression_',
    'Per 90 Minutes_',
    'Per 90_',
    # Possession subcategories
    'Touches_',
    'Take-Ons_',
    'Carries_',
    'Receiving_',
    # Goal/Shot creation subcategories
    'SCA Types_',
    'GCA Types_',
    'SCA_',
    'GCA_',
    # Defensive subcategories
    'Tackles_',
    'Challenges_',
    'Blocks_',
    'Int_',
    'Aerial Duels_',
    'Aerials_',
    # Goalkeeper stats
    'Goalkeeping_',
    'Keeper_',
    'Goal Kicks_',
    'Launched_',
    'Passes_',
    'Crosses_',
    'Sweeper_',
    'Penalty Kicks_',
    'PKs_',
    # Shot events
    'Shot_',
    'Shots_',
    # Match stats
    'Team_',
    'Home_',
    'Away_',
    'Opponent_',
    'vs_',
]


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


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize column names after MultiIndex flatten.

    FBref tables have MultiIndex headers like:
        ('Unnamed: 0_level_0', 'Player') -> 'Unnamed: 0_level_0_Player'
        ('Standard', 'Squad') -> 'Standard_Squad'
        ('Shooting', 'Sh') -> 'Shooting_Sh'

    This function removes these prefixes so columns become:
        'Player', 'Squad', 'Sh'

    Also handles complex cases like:
        'Unnamed: 1_level_0_Player' -> 'Player'
        'Performance_Gls_level_1' -> 'Gls'
        'playing time_mp' -> 'Mp' (lowercase from HTML)

    Args:
        df: DataFrame with flattened MultiIndex columns

    Returns:
        DataFrame with normalized column names
    """
    import re

    if df.empty:
        return df

    rename_map = {}
    seen_names = {}

    for col in df.columns:
        new_col = str(col)
        original_col = col
        col_lower = new_col.lower()

        # First pass: Remove known prefixes (case-insensitive)
        for prefix in MULTIINDEX_PREFIXES:
            prefix_lower = prefix.lower()
            if col_lower.startswith(prefix_lower):
                # Remove prefix keeping original case of remaining part
                new_col = new_col[len(prefix):]
                col_lower = new_col.lower()
                break

        # Second pass: Handle remaining level_X patterns (case-insensitive)
        # Pattern: anything_level_X_ at the start or end
        new_col = re.sub(r'(?i)^.*_level_\d+_', '', new_col)
        new_col = re.sub(r'(?i)_level_\d+$', '', new_col)

        # Third pass: Remove any remaining Unnamed: patterns (case-insensitive)
        new_col = re.sub(r'(?i)^unnamed:\s*\d+_?', '', new_col)

        # Clean up any double underscores or trailing underscores/spaces
        new_col = re.sub(r'_+', '_', new_col)
        new_col = new_col.strip(' _')

        # Capitalize first letter if it's lowercase
        # But preserve intentional lowercase-start names like 'xG', 'xA'
        # (detected by lowercase first letter followed by uppercase)
        if new_col and new_col[0].islower():
            # Only capitalize if it's NOT a pattern like 'xG', 'xA' (lowercase + uppercase)
            if len(new_col) < 2 or not new_col[1].isupper():
                new_col = new_col[0].upper() + new_col[1:]

        # If empty after cleaning, keep original
        if not new_col:
            new_col = str(original_col)

        # Handle duplicates by tracking seen names
        if new_col in seen_names:
            seen_names[new_col] += 1
            new_col = f"{new_col}_{seen_names[new_col]}"
        else:
            seen_names[new_col] = 0

        if new_col != str(original_col):
            rename_map[original_col] = new_col

    if rename_map:
        df = df.rename(columns=rename_map)
        logger.debug(f"Normalized {len(rename_map)} column names: {list(rename_map.items())[:5]}...")

    return df


def extract_tables_from_comments(soup: BeautifulSoup) -> Dict[str, BeautifulSoup]:
    """
    Extract tables hidden in HTML comments.

    FBref hides some tables in HTML comments for lazy loading.
    This method finds and parses those hidden tables.

    Args:
        soup: BeautifulSoup object of the page

    Returns:
        Dictionary mapping table ID to table soup
    """
    tables = {}

    # Find all comments
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    logger.debug(f"Found {len(comments)} HTML comments to check for tables")

    tables_found_count = 0
    comments_processed = 0
    for comment in comments:
        comment_text = str(comment)

        # Check if comment contains a table
        if '<table' in comment_text:
            # Parse the comment as HTML
            comment_soup = BeautifulSoup(comment_text, 'html.parser')
            table = comment_soup.find('table')

            if table:
                table_id = table.get('id', '')
                if table_id:
                    # Extract table before decomposing comment_soup
                    tables[table_id] = table.extract()
                    tables_found_count += 1
                    logger.debug(f"Extracted table from comment: {table_id}")

            # Clean up comment soup to free memory
            comment_soup.decompose()

        comments_processed += 1
        # Periodic garbage collection for large pages
        if comments_processed % 50 == 0:
            gc.collect()

    if tables_found_count > 0:
        logger.info(f"Extracted {tables_found_count} tables from HTML comments: {list(tables.keys())}")
    else:
        logger.warning("No tables found in HTML comments")

    return tables


def diagnose_html_structure(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Diagnose HTML structure for debugging.

    Args:
        soup: BeautifulSoup object

    Returns:
        Dictionary with diagnostic info
    """
    diagnosis = {
        'title': soup.title.string if soup.title else None,
        'total_tables': len(soup.find_all('table')),
        'table_ids': [],
        'has_content_div': soup.find('div', id='content') is not None,
        'has_stats_tables': False,
        'has_sched_tables': False,
        'comment_count': 0,
        'cloudflare_indicators': [],
    }

    # Check for tables
    for table in soup.find_all('table'):
        table_id = table.get('id', '')
        if table_id:
            diagnosis['table_ids'].append(table_id)
            if 'stats' in table_id.lower():
                diagnosis['has_stats_tables'] = True
            if 'sched' in table_id.lower():
                diagnosis['has_sched_tables'] = True

    # Check comments
    comments = soup.find_all(string=lambda text: isinstance(text, Comment))
    diagnosis['comment_count'] = len(comments)

    # Check for Cloudflare indicators
    cf_indicators = [
        'cf-browser-verification', 'challenge-running',
        'ray_id', 'cf-wrapper'
    ]
    for indicator in cf_indicators:
        if soup.find(id=indicator) or soup.find(class_=indicator):
            diagnosis['cloudflare_indicators'].append(indicator)

    # Check title for Cloudflare
    title_lower = (soup.title.string or '').lower() if soup.title else ''
    if any(cf in title_lower for cf in ['just a moment', 'please wait', 'attention required']):
        diagnosis['cloudflare_indicators'].append(f'title:{title_lower}')

    return diagnosis


def parse_table(
    soup: BeautifulSoup,
    table_id: str,
    comment_tables: Optional[Dict[str, BeautifulSoup]] = None,
    extract_player_ids: bool = False,
    extract_team_ids: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Parse HTML table to DataFrame.

    Args:
        soup: BeautifulSoup object
        table_id: ID of the table to parse
        comment_tables: Tables extracted from comments
        extract_player_ids: If True, extract player_id from player links
                           and add as a column to the DataFrame
        extract_team_ids: If True, extract team_id from squad links
                         and add as a column to the DataFrame

    Returns:
        DataFrame or None
    """
    # First try to find table in regular HTML
    table = soup.find('table', id=table_id)

    # If not found, check comment tables
    if table is None and comment_tables:
        table = comment_tables.get(table_id)

    if table is None:
        logger.debug(f"Table not found: {table_id}")
        return None

    # Extract player IDs before parsing with pandas (if requested)
    player_ids_map = {}
    if extract_player_ids:
        player_ids_map = extract_player_ids_from_table(table)

    # Extract team IDs before parsing with pandas (if requested)
    team_ids_map = {}
    if extract_team_ids:
        team_ids_map = extract_team_ids_from_table(table)

    try:
        # Parse table with pandas (use lxml flavor for compatibility with bs4 4.13+)
        html_str = str(table)
        dfs = pd.read_html(StringIO(html_str), flavor='lxml')

        if dfs:
            df = dfs[0]

            # Handle multi-level columns
            if isinstance(df.columns, pd.MultiIndex):
                # Flatten multi-level columns
                df.columns = [
                    '_'.join(col).strip('_')
                    for col in df.columns.values
                ]
                # Normalize column names after flatten (remove prefixes)
                df = normalize_column_names(df)

            # Add player_id column BEFORE filtering (indices must match original HTML rows)
            if extract_player_ids and player_ids_map:
                df['player_id'] = df.index.map(
                    lambda idx: player_ids_map.get(idx)
                )
                logger.debug(
                    f"Added player_id column: "
                    f"{df['player_id'].notna().sum()}/{len(df)} rows have IDs"
                )

            # Add team_id column BEFORE filtering (indices must match original HTML rows)
            if extract_team_ids and team_ids_map:
                df['team_id'] = df.index.map(
                    lambda idx: team_ids_map.get(idx)
                )
                logger.debug(
                    f"Added team_id column: "
                    f"{df['team_id'].notna().sum()}/{len(df)} rows have IDs"
                )

            # Remove header rows repeated inside tbody (where Player == 'Player')
            # These are FBref's inline headers that pd.read_html includes
            if 'Player' in df.columns:
                df = df[df['Player'] != 'Player']

            # Remove header rows for team tables (where Squad == 'Squad')
            if 'Squad' in df.columns:
                df = df[df['Squad'] != 'Squad']
                # Remove summary rows (usually contain 'Squad Total' or similar)
                df = df[
                    ~df['Squad'].str.contains(
                        'Total|Average', na=False, case=False
                    )
                ]

            # Reset index after filtering
            df = df.reset_index(drop=True)

            return df

    except Exception as e:
        logger.error(f"Error parsing table {table_id}: {e}")

    return None


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

    import re
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


def _parse_table_element(
    table: BeautifulSoup,
    extract_player_ids: bool = False,
    extract_team_ids: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Parse table element directly (without lookup by ID).

    Args:
        table: BeautifulSoup table element
        extract_player_ids: If True, extract player_id from player links
        extract_team_ids: If True, extract team_id from squad links

    Returns:
        DataFrame or None
    """
    try:
        # Extract player IDs before parsing with pandas (if requested)
        player_ids_map = {}
        if extract_player_ids:
            player_ids_map = extract_player_ids_from_table(table)

        # Extract team IDs before parsing with pandas (if requested)
        team_ids_map = {}
        if extract_team_ids:
            team_ids_map = extract_team_ids_from_table(table)

        html_str = str(table)
        dfs = pd.read_html(StringIO(html_str), flavor='lxml')

        if dfs:
            df = dfs[0]

            # Handle multi-level columns
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [
                    '_'.join(col).strip('_')
                    for col in df.columns.values
                ]
                df = normalize_column_names(df)

            # Add player_id column BEFORE filtering (indices must match original HTML rows)
            if extract_player_ids and player_ids_map:
                df['player_id'] = df.index.map(
                    lambda idx: player_ids_map.get(idx)
                )

            # Add team_id column BEFORE filtering (indices must match original HTML rows)
            if extract_team_ids and team_ids_map:
                df['team_id'] = df.index.map(
                    lambda idx: team_ids_map.get(idx)
                )

            # Remove header rows repeated inside tbody (where Player == 'Player')
            if 'Player' in df.columns:
                df = df[df['Player'] != 'Player']

            # Remove header rows for team tables (where Squad == 'Squad')
            if 'Squad' in df.columns:
                df = df[df['Squad'] != 'Squad']
                # Remove summary rows
                df = df[
                    ~df['Squad'].str.contains(
                        'Total|Average', na=False, case=False
                    )
                ]

            # Reset index after filtering
            df = df.reset_index(drop=True)

            return df

    except Exception as e:
        logger.debug(f"Error parsing table element: {e}")

    return None


def _table_has_player_header(table: BeautifulSoup) -> bool:
    """
    Check if table has a 'Player' column header.

    Args:
        table: BeautifulSoup table element

    Returns:
        True if table has 'Player' header
    """
    thead = table.find('thead')
    if thead:
        for th in thead.find_all('th'):
            if th.get_text(strip=True).lower() == 'player':
                return True
    return False


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
    import re

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
    import re

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
        elif 'yellow' in str(div).lower() or '🟨' in text:
            event_type = 'yellow_card'
        elif 'red' in str(div).lower() or '🟥' in text:
            event_type = 'red_card'
        elif 'sub' in str(div).lower() or '🔃' in text:
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
