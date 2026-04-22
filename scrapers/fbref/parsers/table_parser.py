"""
Table Parser
=============

Core HTML table parsing utilities for FBref pages, including
column normalization, table extraction from HTML comments,
and DataFrame conversion.
"""

import gc
import logging
import re
from io import StringIO
from typing import Any, Dict, Optional

import pandas as pd
from bs4 import BeautifulSoup, Comment

from scrapers.fbref.parsers.id_extractors import (
    extract_player_ids_from_table,
    extract_team_ids_from_table,
)

logger = logging.getLogger(__name__)


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
    # Playing Time subcategories
    'Starts_',
    'Subs_',
    'Team Success_',
    'Team Success (xG)_',
    # Passing Types subcategories
    'Corner Kicks_',
    'Outcomes_',
    'Height_',
    'Body Parts_',
]


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

    # Diagnostic: log columns that may still have unstripped prefixes
    remaining_prefixed = [
        c for c in df.columns
        if '_' in str(c) and not str(c).startswith('_')
        and str(c).lower() not in ('player_id', 'team_id', 'stat_type')
    ]
    if remaining_prefixed:
        logger.info(f"Columns with potential unstripped prefixes: {remaining_prefixed[:10]}")

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
        # Check if DOM already has tables (e.g. uncommented by pre_content_js)
        dom_tables = soup.find_all('table')
        if len(dom_tables) > 0:
            logger.debug(
                f"No tables in HTML comments, but {len(dom_tables)} tables "
                f"already present in DOM (likely uncommented by pre_content_js)"
            )
        else:
            comments_with_table = sum(1 for c in comments if '<table' in str(c))
            logger.warning(
                f"No tables found in HTML comments or DOM. "
                f"Total comments: {len(comments)}, "
                f"comments containing '<table': {comments_with_table}. "
                f"Possible causes: (1) Cloudflare block returned empty page, "
                f"(2) browser closed before page fully loaded, "
                f"(3) page structure changed."
            )

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
