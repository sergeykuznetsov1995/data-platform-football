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
from scrapers.fbref.parsers.id_extractors import (
    MANAGER_ID_PATTERN,
    PLAYER_ID_PATTERN,
    TEAM_ID_PATTERN,
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
        df = parse_table(soup, table_id, comment_tables, extract_match_urls=True)
        if df is not None and not df.empty:
            return df

    # Try finding any table with 'sched' in ID from regular HTML
    all_tables = soup.find_all(
        'table',
        id=lambda x: x and 'sched' in x.lower()
    )
    for table in all_tables:
        table_id = table.get('id')
        df = parse_table(soup, table_id, comment_tables, extract_match_urls=True)
        if df is not None and not df.empty:
            logger.debug(f"Found schedule table: {table_id}")
            return df

    # Try parsing any table with 'sched' in ID from comments
    for key in comment_tables.keys():
        if 'sched' in key.lower():
            df = parse_table(soup, key, comment_tables, extract_match_urls=True)
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
        f'stats_squads_{fbref_stat_type}',
        f'stats_squads_{stat_type}',
    ]
    # Only add standard fallback for standard stats — otherwise it returns
    # the wrong table (Standard Stats instead of the target stat type)
    if stat_type in ('stats', 'standard'):
        table_ids.insert(2, 'stats_squads_standard_for')

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
        f'stats_{fbref_stat_type}_all',
        f'stats_{stat_type}_all',
    ]
    # Only add standard fallback for standard stats — otherwise it returns
    # the wrong table (Standard Stats instead of the target stat type)
    if stat_type in ('stats', 'standard'):
        table_ids.append('stats_standard')

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

        # Option 1: Table with ID containing 'stats' (excluding squad/results/nations)
        if table_id and 'stats' in table_id.lower() and 'squad' not in table_id.lower():
            # Must have 'Player' header — skip league tables, results tables, etc.
            if not _table_has_player_header(table):
                continue
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
            # Must have 'Player' header — skip league tables, results tables, etc.
            if not _table_has_player_header(table):
                continue
            df = _parse_table_element(table, extract_player_ids)
            if df is not None and not df.empty:
                logger.info(f"Found stats table by class: {table_class_str}")
                return df

    # Option 3: Try comment tables with any stats-related key
    for key in comment_tables.keys():
        if 'stats' in key.lower() and 'squad' not in key.lower():
            df = parse_table(
                soup, key, comment_tables,
                extract_player_ids=extract_player_ids
            )
            if df is not None and not df.empty:
                logger.info(f"Found player stats table in comments: {key}")
                return df

    # No "any table with a Player header" last resort: on an FBref layout
    # change it silently returned the WRONG stat table (e.g. misc rows filed
    # into the shooting bronze table). Fail loudly instead.
    logger.error(
        f"No player stats table found for stat_type={stat_type!r}. "
        f"DOM table IDs: {[t.get('id', '<no-id>') for t in all_tables]}, "
        f"comment table IDs: {list(comment_tables.keys())}"
    )
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
        if df is not None:
            if df.empty:
                logger.info(
                    f"Shots table '{table_id}' found but empty — "
                    f"likely FBref data restrictions (Feb 2026+)"
                )
                return None
            logger.debug(f"Found shots table: {table_id}")
            return df

    # Try finding any table with 'shots' in ID
    for table in soup.find_all('table'):
        table_id = table.get('id', '')
        if 'shot' in table_id.lower():
            df = parse_table(soup, table_id, comment_tables)
            if df is not None:
                if df.empty:
                    logger.info(
                        f"Shots table '{table_id}' found in DOM but empty"
                    )
                    return None
                return df

    # Check comment tables
    for key in comment_tables.keys():
        if 'shot' in key.lower():
            df = parse_table(soup, key, comment_tables)
            if df is not None:
                if df.empty:
                    logger.info(
                        f"Shots table '{key}' found in comments but empty"
                    )
                    return None
                return df

    # Diagnostic logging: no shots table found at all
    all_table_ids = [
        t.get('id', '<no-id>') for t in soup.find_all('table')
    ]
    comment_keys = list(comment_tables.keys())
    logger.debug(
        f"No shots table found. "
        f"DOM table IDs: {all_table_ids}, "
        f"Comment table keys: {comment_keys}"
    )

    return None


def parse_lineup_table(
    soup: BeautifulSoup,
    team_id: Optional[str] = None,
    comment_tables: Optional[Dict[str, BeautifulSoup]] = None,
) -> Optional[pd.DataFrame]:
    """
    Parse lineup information from match page.

    FBref lineups are in ``div.lineup`` elements, each containing a
    ``<table>`` with rows for starters followed by a ``<th>Bench</th>``
    header row and then bench players.  Positions are NOT stored in the
    lineup div — they come from ``stats_{team_id}_summary`` tables
    (often inside HTML comments).

    Args:
        soup: BeautifulSoup object of match page
        team_id: Optional team ID to filter specific team
        comment_tables: Tables extracted from HTML comments (needed
            to look up player positions from summary tables)

    Returns:
        DataFrame with lineup data or None
    """
    lineups = []

    # Find lineup divs
    lineup_divs = soup.find_all('div', class_='lineup')

    if not lineup_divs:
        lineup_divs = soup.find_all(
            'div',
            class_=lambda x: x and 'lineup' in str(x).lower(),
        )
    if not lineup_divs:
        lineup_divs = soup.find_all(
            'div',
            id=lambda x: x and 'lineup' in str(x).lower(),
        )

    if not lineup_divs:
        logger.debug("No lineup divs found on page")
        return None

    for div in lineup_divs:
        # Look for <table> inside the lineup div
        table = div.find('table')
        if not table:
            # No table — try parsing player links directly
            table = div

        # Extract team name from first <th> header
        team_name = 'Unknown'
        team_header = div.find('th')
        if not team_header:
            team_header = div.find(['h2', 'h3'])
        if team_header:
            header_text = team_header.get_text(strip=True)
            # Strip formation like "(4-3-3)" from team name
            team_name = re.sub(r'\s*\([\d\-]+\)\s*$', '', header_text).strip()
            if not team_name:
                team_name = header_text

        # Parse table rows; track starter/bench via "Bench" header
        is_bench = False
        rows = table.find_all('tr') if table.name == 'table' else []

        for row in rows:
            # Check for section header (team name or "Bench")
            th = row.find('th')
            if th:
                th_text = th.get_text(strip=True)
                if th_text.lower().startswith('bench'):
                    is_bench = True
                    continue
                # Skip team-name / formation header row
                continue

            # Find player link in row
            player_link = row.find(
                'a', href=lambda x: x and '/players/' in str(x)
            )
            if not player_link:
                continue

            player_name = player_link.get_text(strip=True)
            href = player_link.get('href', '')

            player_id = None
            m = PLAYER_ID_PATTERN.search(href)
            if m:
                player_id = m.group(1)

            # Jersey number from first <td> (if it's a digit)
            number = ''
            tds = row.find_all('td')
            if tds:
                first_td_text = tds[0].get_text(strip=True)
                if first_td_text.isdigit():
                    number = first_td_text

            lineups.append({
                'team': team_name,
                'player': player_name,
                'player_id': player_id,
                'is_starter': not is_bench,
                'position': '',
                'number': number,
            })

    if not lineups:
        return None

    df = pd.DataFrame(lineups)

    # Enrich positions from stats_*_summary tables
    df = _enrich_lineup_positions(soup, comment_tables, df)

    return df


def _enrich_lineup_positions(
    soup: BeautifulSoup,
    comment_tables: Optional[Dict[str, BeautifulSoup]],
    lineup_df: pd.DataFrame,
) -> pd.DataFrame:
    """Enrich lineup DataFrame with positions from stats summary tables.

    FBref stores player positions in ``stats_{team_id}_summary`` tables
    (usually inside HTML comments).  Each row has
    ``<td data-stat="position">GK</td>``.  We build a mapping of
    ``player_id -> position`` and merge it into the lineup DataFrame.
    """
    if lineup_df is None or lineup_df.empty:
        return lineup_df

    position_map: Dict[str, str] = {}

    tables_to_check = []

    # DOM tables
    for table in soup.find_all('table'):
        table_id = table.get('id', '')
        if 'summary' in table_id.lower() and 'stats' in table_id.lower():
            tables_to_check.append(table)

    # Comment tables
    if comment_tables:
        for key, table in comment_tables.items():
            if 'summary' in key.lower() and 'stats' in key.lower():
                tables_to_check.append(table)

    for table in tables_to_check:
        tbody = table.find('tbody')
        if not tbody:
            continue

        for row in tbody.find_all('tr'):
            row_class = row.get('class', [])
            if isinstance(row_class, str):
                row_class = [row_class]
            if any(c in row_class for c in ['spacer', 'thead', 'over_header']):
                continue

            player_link = row.find(
                'a', href=lambda x: x and '/players/' in str(x)
            )
            if not player_link:
                continue

            href = player_link.get('href', '')
            m = PLAYER_ID_PATTERN.search(href)
            if not m:
                continue

            pid = m.group(1)

            pos_cell = row.find(['td', 'th'], attrs={'data-stat': 'position'})
            if pos_cell:
                position = pos_cell.get_text(strip=True)
                if position and pid not in position_map:
                    position_map[pid] = position

    if position_map:
        lineup_df['position'] = lineup_df.apply(
            lambda r: position_map.get(r.get('player_id', ''), r.get('position', '')),
            axis=1,
        )
        enriched = sum(
            1 for _, r in lineup_df.iterrows()
            if r.get('player_id') in position_map
        )
        logger.debug(
            f"Enriched {enriched}/{len(lineup_df)} lineup positions "
            f"from {len(tables_to_check)} summary tables"
        )

    return lineup_df


def parse_events_from_scorebox(soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    """
    Parse match events (goals, cards, substitutions) from match page.

    FBref stores events in ``#events_wrap`` div (not the scorebox).
    Each ``div.event`` contains a single player event with class ``a``
    (home) or ``b`` (away) indicating the team side.

    Minutes use U+02BC (``\u02bc``) modifier letter apostrophe, not
    ASCII ``'``.

    Args:
        soup: BeautifulSoup object of match page

    Returns:
        DataFrame with match events or None
    """
    events = []

    # Primary: #events_wrap contains individual div.event elements
    events_wrap = soup.find('div', id='events_wrap')

    if events_wrap:
        event_divs = events_wrap.find_all('div', class_='event')
    else:
        # Fallback: scorebox (legacy / alternative layout)
        scorebox = soup.find('div', class_='scorebox')
        if not scorebox:
            scorebox = soup.find('div', id='scorebox')
        if not scorebox:
            logger.debug("No events_wrap or scorebox found on page")
            return None
        event_divs = scorebox.find_all(
            'div',
            class_=lambda x: x and 'event' in str(x).lower(),
        )

    if not event_divs:
        logger.debug("No event divs found")
        return None

    # Extract team names from scorebox for enrichment
    team_names = _extract_team_names_from_scorebox(soup)

    # Regex for minutes — FBref uses various apostrophe-like characters:
    # U+02BC (ʼ) modifier apostrophe, U+0027 (') ASCII, U+2032 (′) prime,
    # U+0060 (`) backtick, U+2018 (') left quote, U+2019 (') right quote
    # FBref format: "45+2'" — apostrophe AFTER the stoppage-time part
    minute_re = re.compile(r"(\d+)(?:\+(\d+))?[\u02bc\u0027\u2032\u0060\u2018\u2019]")

    for div in event_divs:
        # Team side from CSS class: 'a' = home, 'b' = away
        div_classes = div.get('class', [])
        if isinstance(div_classes, str):
            div_classes = [div_classes]

        team_side = ''
        if 'a' in div_classes:
            team_side = 'home'
        elif 'b' in div_classes:
            team_side = 'away'

        # Minute — search all text in the event div
        text = div.get_text(' ', strip=True)
        minute = ''
        minute_match = minute_re.search(text)
        if minute_match:
            minute = minute_match.group(1)
            if minute_match.group(2):
                minute = f"{minute}+{minute_match.group(2)}"

        # Event type from CSS classes of inner elements
        event_type = _detect_event_type(div)

        # Player links
        player_links = div.find_all(
            'a', href=lambda x: x and '/players/' in str(x)
        )

        player_name = ''
        player_id = None
        secondary_player = ''
        secondary_player_id = None

        if player_links:
            # First link = main player (scorer / carded / subbed-in)
            first_link = player_links[0]
            player_name = first_link.get_text(strip=True)
            href = first_link.get('href', '')
            m = PLAYER_ID_PATTERN.search(href)
            if m:
                player_id = m.group(1)

            # Second link = assist / subbed-off player
            if len(player_links) > 1:
                second_link = player_links[1]
                secondary_player = second_link.get_text(strip=True)
                href2 = second_link.get('href', '')
                m2 = PLAYER_ID_PATTERN.search(href2)
                if m2:
                    secondary_player_id = m2.group(1)

        # Resolve team name
        team = ''
        if team_side == 'home' and team_names.get('home'):
            team = team_names['home']
        elif team_side == 'away' and team_names.get('away'):
            team = team_names['away']

        if minute or player_name or event_type != 'unknown':
            events.append({
                'minute': minute,
                'event_type': event_type,
                'player': player_name,
                'player_id': player_id,
                'team': team,
                'team_side': team_side,
                'secondary_player': secondary_player,
                'secondary_player_id': secondary_player_id,
            })

    if events:
        return pd.DataFrame(events)

    return None


def _extract_team_names_from_scorebox(
    soup: BeautifulSoup,
) -> Dict[str, str]:
    """Extract home/away team names from scorebox ``/squads/`` links."""
    result: Dict[str, str] = {'home': '', 'away': ''}
    scorebox = soup.find('div', class_='scorebox')
    if not scorebox:
        return result

    team_links = scorebox.find_all(
        'a', href=lambda x: x and '/squads/' in str(x)
    )
    if len(team_links) >= 2:
        result['home'] = team_links[0].get_text(strip=True)
        result['away'] = team_links[1].get_text(strip=True)
    elif len(team_links) == 1:
        result['home'] = team_links[0].get_text(strip=True)

    return result


def _extract_team_ids_from_scorebox(
    soup: BeautifulSoup,
) -> Dict[str, str]:
    """Extract home/away team_ids from scorebox ``/squads/{id}/`` links."""
    result: Dict[str, str] = {'home': '', 'away': ''}
    scorebox = soup.find('div', class_='scorebox')
    if not scorebox:
        return result

    ids = []
    for link in scorebox.find_all(
        'a', href=lambda x: x and '/squads/' in str(x)
    ):
        m = TEAM_ID_PATTERN.search(link.get('href', ''))
        if m:
            ids.append(m.group(1))
    if ids:
        result['home'] = ids[0]
    if len(ids) >= 2:
        result['away'] = ids[1]
    return result


# {team_id} embedded in match-page table ids: stats_{team_id}_summary,
# keeper_stats_{team_id}. Boundary-anchored so a longer hex run can't match.
_TABLE_TEAM_ID_RE = re.compile(r'_([a-f0-9]{8})(?:_|$)')


def _side_for_table_id(
    table_id: str, team_ids_by_side: Dict[str, str]
) -> Optional[str]:
    """Resolve 'home'/'away' from the {team_id} embedded in a table id.

    Table order (DOM first, then comment tables) is NOT a reliable side
    signal on partially-uncommented pages — matching the id against the
    scorebox squads is. Returns None when the id matches neither side.
    """
    m = _TABLE_TEAM_ID_RE.search(table_id)
    if not m:
        return None
    tid = m.group(1)
    if tid and tid == team_ids_by_side.get('home'):
        return 'home'
    if tid and tid == team_ids_by_side.get('away'):
        return 'away'
    return None


def _detect_event_type(div) -> str:
    """Detect event type from CSS classes of nested elements and text."""
    # Check inner div/span classes
    for child in div.find_all(['div', 'span']):
        child_classes = child.get('class', [])
        if isinstance(child_classes, str):
            child_classes = [child_classes]
        cls_str = ' '.join(child_classes).lower()

        if 'own_goal' in cls_str or 'own-goal' in cls_str:
            return 'own_goal'
        # Missed penalty MUST be checked before the generic 'penalty' branch:
        # FBref marks it with the `penalty_miss` sprite, whose class also
        # contains the substring 'penalty' and would otherwise collapse into a
        # scored penalty and inflate the downstream running score. See #447.
        if 'penalty_miss' in cls_str or 'penalty-miss' in cls_str:
            return 'penalty_missed'
        if 'penalty' in cls_str:
            return 'penalty'
        if 'goal' in cls_str:
            # Check text for penalty/own-goal indicators
            child_text = child.get_text(strip=True).lower()
            if 'penalty' in child_text or '(pen.)' in child_text:
                return 'penalty'
            if 'own goal' in child_text or '(o.g.)' in child_text:
                return 'own_goal'
            return 'goal'
        if 'second_yellow' in cls_str or 'second-yellow' in cls_str:
            return 'second_yellow_card'
        # Second yellow = sending off. FBref marks it with the `yellow_red_card`
        # sprite; map to second_yellow_card (NOT yellow_card) so the category is
        # populated from FBref. See #447. (_parse_team_stats_main already counts
        # `yellow_red` as a red at team level.)
        if 'yellow_red' in cls_str or 'yellow-red' in cls_str:
            return 'second_yellow_card'
        if 'yellow_card' in cls_str or 'yellow-card' in cls_str:
            return 'yellow_card'
        if 'red_card' in cls_str or 'red-card' in cls_str:
            return 'red_card'
        if 'substitute' in cls_str or cls_str.startswith('sub'):
            return 'substitution'

    # Fallback: text-based detection
    text = div.get_text(strip=True).lower()
    if 'penalty' in text or '(pen.)' in text:
        return 'penalty'
    if 'own goal' in text or '(o.g.)' in text:
        return 'own_goal'

    div_str = str(div).lower()
    if 'goal' in div_str:
        return 'goal'
    if 'yellow' in div_str:
        return 'yellow_card'
    if 'red' in div_str:
        return 'red_card'
    if 'sub' in div_str:
        return 'substitution'

    return 'unknown'


# ------------------------------------------------------------------
# Helpers for team match stats parsing
# ------------------------------------------------------------------

def _safe_int(text) -> int:
    """Safely convert text to int, stripping non-digit characters."""
    try:
        digits = re.sub(r'[^\d]', '', str(text).strip())
        return int(digits) if digits else 0
    except (ValueError, TypeError):
        return 0


def _parse_pct(text: str) -> int:
    """Parse percentage text: ``'61%'`` -> ``61``."""
    m = re.search(r'(\d+)\s*%', str(text))
    return int(m.group(1)) if m else 0


def _parse_of_stat(text: str) -> tuple:
    """Parse ``'X of Y — Z%'`` or ``'Z% — X of Y'`` -> ``(X, Y)``."""
    m = re.search(r'(\d+)\s+of\s+(\d+)', str(text))
    return (int(m.group(1)), int(m.group(2))) if m else (0, 0)


def _parse_team_stats_main(team_stats_div, result: dict) -> None:
    """Parse the ``<table>`` inside ``div#team_stats``.

    Iterates ``<tr>`` rows: header rows (``<th>``) carry the stat label,
    the immediately following data row holds the values.
    """
    table = team_stats_div.find('table')
    if not table:
        return

    current_label = None
    for row in table.find_all('tr'):
        th = row.find('th')
        if th:
            current_label = th.get_text(strip=True).lower()
            continue

        if current_label is None:
            continue

        tds = row.find_all('td')
        if len(tds) < 2:
            current_label = None
            continue

        home_text = tds[0].get_text(strip=True)
        away_text = tds[1].get_text(strip=True)

        if 'possession' in current_label:
            result['home_possession'] = _parse_pct(home_text)
            result['away_possession'] = _parse_pct(away_text)

        elif 'shots on target' in current_label:
            home_sot, home_shots = _parse_of_stat(home_text)
            away_sot, away_shots = _parse_of_stat(away_text)
            result['home_shots'] = home_shots
            result['away_shots'] = away_shots
            result['home_sot'] = home_sot
            result['away_sot'] = away_sot

        elif current_label.startswith('save'):
            home_saves, _ = _parse_of_stat(home_text)
            away_saves, _ = _parse_of_stat(away_text)
            result['home_saves'] = home_saves
            result['away_saves'] = away_saves

        elif 'card' in current_label:
            for prefix, td in [('home', tds[0]), ('away', tds[1])]:
                all_spans = td.find_all('span', class_=True)
                yellows = 0
                reds = 0
                for span in all_spans:
                    cls = ' '.join(span.get('class', []))
                    if 'yellow_red' in cls or 'yellow-red' in cls:
                        reds += 1
                    elif 'red' in cls:
                        reds += 1
                    elif 'yellow' in cls:
                        yellows += 1
                result[f'{prefix}_yellow_cards'] = yellows
                result[f'{prefix}_red_cards'] = reds

        current_label = None


def _parse_team_stats_extra(extra_div, result: dict) -> None:
    """Parse ``div#team_stats_extra``: groups of (home_val, label, away_val)."""
    label_map = {
        'fouls': 'fouls',
        'corners': 'corners',
        'crosses': 'crosses',
        'interceptions': 'interceptions',
        'offsides': 'offsides',
        'touches': 'touches',
        'tackles': 'tackles',
        'aerials won': 'aerials_won',
        'clearances': 'clearances',
        'goal kicks': 'goal_kicks',
        'throw ins': 'throw_ins',
    }

    for group_div in extra_div.find_all('div', recursive=False):
        children = group_div.find_all('div', recursive=False)
        if len(children) < 3:
            continue

        home_text = children[0].get_text(strip=True)
        label_text = children[1].get_text(strip=True).lower()
        away_text = children[2].get_text(strip=True)

        col_stem = label_map.get(label_text)
        if col_stem:
            result[f'home_{col_stem}'] = _safe_int(home_text)
            result[f'away_{col_stem}'] = _safe_int(away_text)


# ------------------------------------------------------------------
# Team match stats: main function
# ------------------------------------------------------------------

def parse_team_match_stats_table(
    soup: BeautifulSoup,
    comment_tables: Dict[str, BeautifulSoup],
    team_id: Optional[str] = None
) -> Optional[pd.DataFrame]:
    """
    Parse team-level match statistics from ``div#team_stats`` and
    ``div#team_stats_extra``.

    FBref stores team match stats in two non-standard div elements
    (not regular ``<table>``).  This function extracts Possession,
    Shots on Target, Saves, Cards, Fouls, Corners, Crosses,
    Interceptions, Offsides, and other available stats.

    Args:
        soup: BeautifulSoup object of match page
        comment_tables: Tables extracted from comments (unused but kept
            for signature compatibility)
        team_id: Optional team ID (unused, kept for backward compat)

    Returns:
        1-row DataFrame with home/away stat columns, or None
    """
    team_stats_div = soup.find('div', id='team_stats')
    if not team_stats_div:
        logger.debug("No div#team_stats found on page")
        return None

    team_names = _extract_team_names_from_scorebox(soup)

    result = {
        'home_team': team_names.get('home', ''),
        'away_team': team_names.get('away', ''),
    }

    _parse_team_stats_main(team_stats_div, result)

    extra_div = soup.find('div', id='team_stats_extra')
    if extra_div:
        _parse_team_stats_extra(extra_div, result)

    # Only return if we actually parsed something beyond team names
    stat_keys = [k for k in result if k not in ('home_team', 'away_team')]
    if not stat_keys:
        logger.debug("div#team_stats found but no stats could be extracted")
        return None

    return pd.DataFrame([result])


# ------------------------------------------------------------------
# Player match stats (per-match individual stats from summary tables)
# ------------------------------------------------------------------

def parse_player_match_stats_tables(
    soup: BeautifulSoup,
    comment_tables: Dict[str, BeautifulSoup],
) -> Optional[pd.DataFrame]:
    """
    Parse player match stats from ``stats_*_summary`` tables.

    FBref match pages contain two summary tables (one per team) with
    per-player stats (minutes, goals, assists, shots, etc.).  Tables
    are identified by IDs like ``stats_{team_id}_summary`` and are
    often inside HTML comments.

    Returns a combined DataFrame with ``team_side`` ('home'/'away')
    and ``team`` columns.  Total/summary rows are filtered out.
    """
    team_names = _extract_team_names_from_scorebox(soup)

    # Collect unique summary table IDs from DOM + comment_tables
    seen_ids: set = set()
    summary_ids: list = []

    for table in soup.find_all('table'):
        table_id = table.get('id', '')
        if table_id and 'summary' in table_id and 'stats' in table_id:
            if table_id not in seen_ids:
                seen_ids.add(table_id)
                summary_ids.append(table_id)

    if comment_tables:
        for table_id in comment_tables:
            if 'summary' in table_id and 'stats' in table_id:
                if table_id not in seen_ids:
                    seen_ids.add(table_id)
                    summary_ids.append(table_id)

    if not summary_ids:
        logger.debug("No stats_*_summary tables found for player match stats")
        return None

    parsed = []
    for table_id in summary_ids:
        df = parse_table(soup, table_id, comment_tables, extract_player_ids=True)
        if df is not None and not df.empty:
            parsed.append((table_id, df))

    if not parsed:
        logger.debug("stats_*_summary tables found but all empty")
        return None

    # Assign team_side by matching the {team_id} in the table id against the
    # scorebox squads; table order is only the fallback (order is unreliable
    # on partially-uncommented pages).
    team_ids_by_side = _extract_team_ids_from_scorebox(soup)
    sides = ['home', 'away']

    result_dfs = []
    for i, (table_id, df) in enumerate(parsed):
        df = df.copy()
        side = _side_for_table_id(table_id, team_ids_by_side)
        if side is None:
            side = sides[i] if i < len(sides) else f'team_{i + 1}'
        df['team_side'] = side
        df['team'] = team_names.get(side, '')
        result_dfs.append(df)

    combined = pd.concat(result_dfs, ignore_index=True)

    # Filter out total/summary rows (e.g. "15 Players")
    if 'Player' in combined.columns:
        total_re = re.compile(r'^\d+\s+Players?$', re.IGNORECASE)
        combined = combined[
            ~combined['Player'].astype(str).str.match(total_re, na=False)
        ]

    if combined.empty:
        return None

    logger.debug(
        f"Parsed {len(combined)} player match stats rows "
        f"from {len(parsed)} summary tables"
    )
    return combined


# ------------------------------------------------------------------
# Keeper match stats (per-match goalkeeper stats from keeper_stats tables)
# ------------------------------------------------------------------

def parse_keeper_match_stats_tables(
    soup: BeautifulSoup,
    comment_tables: Dict[str, BeautifulSoup],
) -> Optional[pd.DataFrame]:
    """
    Parse goalkeeper match stats from ``keeper_stats_{team_id}`` tables.

    FBref match pages contain one goalkeeper table per team. Post the
    Apr-2026 FBref restriction the advanced columns (PSxG, launches,
    sweeper) are gone, but the basic ones (SoTA, GA, Saves, Save%) are
    still populated — verified on a 2026-05-24 match page.

    Returns a combined DataFrame with ``team_side`` ('home'/'away')
    and ``team`` columns, mirroring parse_player_match_stats_tables.
    """
    team_names = _extract_team_names_from_scorebox(soup)

    seen_ids: set = set()
    keeper_ids: list = []

    for table in soup.find_all('table'):
        table_id = table.get('id', '')
        if table_id and 'keeper_stats' in table_id:
            if table_id not in seen_ids:
                seen_ids.add(table_id)
                keeper_ids.append(table_id)

    if comment_tables:
        for table_id in comment_tables:
            if 'keeper_stats' in table_id:
                if table_id not in seen_ids:
                    seen_ids.add(table_id)
                    keeper_ids.append(table_id)

    if not keeper_ids:
        logger.debug("No keeper_stats_* tables found for keeper match stats")
        return None

    parsed = []
    for table_id in keeper_ids:
        df = parse_table(soup, table_id, comment_tables, extract_player_ids=True)
        if df is not None and not df.empty:
            parsed.append((table_id, df))

    if not parsed:
        logger.debug("keeper_stats_* tables found but all empty")
        return None

    # Side by {team_id} in the table id, order only as fallback — mirrors
    # parse_player_match_stats_tables.
    team_ids_by_side = _extract_team_ids_from_scorebox(soup)
    sides = ['home', 'away']

    result_dfs = []
    for i, (table_id, df) in enumerate(parsed):
        df = df.copy()
        side = _side_for_table_id(table_id, team_ids_by_side)
        if side is None:
            side = sides[i] if i < len(sides) else f'team_{i + 1}'
        df['team_side'] = side
        df['team'] = team_names.get(side, '')
        result_dfs.append(df)

    combined = pd.concat(result_dfs, ignore_index=True)

    if combined.empty:
        return None

    logger.debug(
        f"Parsed {len(combined)} keeper match stats rows "
        f"from {len(parsed)} keeper tables"
    )
    return combined


# Match scorebox label "Manager:" / "Manager :" / "MANAGER:" etc.
# FBref's current scorebox renders the label as
# ``<div class="datapoint"><strong>Manager</strong>: Name</div>`` —
# BeautifulSoup's get_text(separator=' ') yields ``"Manager : Name"`` with
# a space before the colon, so the regex must tolerate optional spaces.
_MANAGER_LABEL_RE = re.compile(r'^\s*Manager\s*:\s*(.+?)\s*$', re.IGNORECASE)


def _norm_name(s: Optional[str]) -> Optional[str]:
    """Normalise a person's name extracted from FBref HTML.

    FBref uses U+00A0 (non-breaking space) inside multi-word names
    ("Arne Slot"). Downstream xref normalisation
    (``LOWER(REGEXP_REPLACE(name, '[^a-zA-Z0-9]+', '_'))``) collapses
    those to underscores anyway, but raw Bronze rows look weird in
    SELECT * output if the NBSP leaks through. Replace with a regular
    space and trim whitespace.
    """
    if s is None:
        return None
    s = s.replace(' ', ' ').strip()
    return s or None


def parse_match_managers(soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    """Parse home/away managers from the FBref match-page scorebox.

    The current FBref scorebox renders each team in a
    ``<div class="scorebox_team">`` block. Inside that block manager info
    sits in
    ``<div class="datapoint"><strong>Manager</strong>: <Name></div>``.
    Older fixtures sometimes wrap the name in an ``<a href="/managers/...">``
    link; we capture that ``manager_fbref_id`` when present but treat it
    as optional.

    Returns a DataFrame with one row per team side. ``manager_name`` /
    ``manager_fbref_id`` are NULL when the scorebox does not list a manager
    for that side (rare; happens on a few historical fixtures). Returning
    NULL rows instead of skipping keeps Bronze schema-stable and lets the
    Silver layer surface the gap via DQ rather than silently dropping.

    Args:
        soup: BeautifulSoup of the match page.

    Returns:
        DataFrame with columns ``side``, ``team``, ``manager_name``,
        ``manager_fbref_id``. ``None`` if no scorebox at all (caller treats
        that as parse failure, not as "no managers").
    """
    scorebox = soup.find('div', class_='scorebox')
    if not scorebox:
        return None

    # Prefer the modern markup (scorebox_team / scorebox_team_b1 etc.).
    # Fall back to the first two direct ``<div>`` children for legacy or
    # synthetic fixtures used in unit tests.
    team_blocks = scorebox.find_all('div', class_='scorebox_team')
    if not team_blocks:
        team_blocks = [
            c for c in scorebox.find_all('div', recursive=False)
            if 'scorebox_meta' not in (c.get('class') or [])
        ][:2]
    team_blocks = team_blocks[:2]
    if not team_blocks:
        return None

    team_names = _extract_team_names_from_scorebox(soup)
    sides = ['home', 'away']
    rows = []

    for i, block in enumerate(team_blocks):
        side = sides[i] if i < len(sides) else f'team_{i + 1}'
        team = team_names.get(side, '')

        manager_name: Optional[str] = None
        manager_id: Optional[str] = None

        for div in block.find_all('div'):
            label_match = _MANAGER_LABEL_RE.match(div.get_text(' ', strip=True))
            if not label_match:
                continue
            manager_name = _norm_name(label_match.group(1))
            link = div.find('a', href=lambda x: x and '/managers/' in str(x))
            if link is not None:
                href = link.get('href', '')
                m = MANAGER_ID_PATTERN.search(href)
                if m:
                    manager_id = m.group(1)
                # Prefer the link text as canonical name (drops trailing
                # punctuation / extra whitespace from the surrounding div).
                link_text = _norm_name(link.get_text(strip=True))
                if link_text:
                    manager_name = link_text
            break

        rows.append({
            'side': side,
            'team': team,
            'manager_name': manager_name,
            'manager_fbref_id': manager_id,
        })

    if not rows:
        return None

    return pd.DataFrame(rows)


# Match scorebox_meta "Officials" line (issue #613). FBref renders each official
# as ``<span style="display:inline-block">Name (Role)</span>`` with roles already
# numbered — (Referee) (AR1) (AR2) (4th) (VAR) — confirmed on a live APL match
# page (Liverpool–Bournemouth, 2026-06-16). The regex tolerates an optional
# space before '(' and is case-insensitive; unknown roles (e.g. AVAR/PR on some
# cup ties) simply fall through and are ignored.
_OFFICIAL_RE = re.compile(
    r'^(.*?)\s*\((Referee|AR1|AR2|4th|VAR)\)\s*$', re.IGNORECASE
)

# FBref role label -> Bronze column (wide, one row per match).
_OFFICIAL_ROLE_TO_COL = {
    'referee': 'referee',
    'ar1': 'ar1',
    'ar2': 'ar2',
    '4th': 'fourth_official',
    'var': 'var',
}

_OFFICIAL_COLUMNS = ['referee', 'ar1', 'ar2', 'fourth_official', 'var']


def parse_match_officials(soup: BeautifulSoup) -> Optional[pd.DataFrame]:
    """Parse the officiating crew from the FBref match-page scorebox_meta.

    The scorebox_meta block renders officials as::

        <strong><small>Officials</small></strong> :
        <small>
          <span style="display:inline-block">Anthony Taylor (Referee)</span> ·
          <span style="display:inline-block">Gary Beswick (AR1)</span> · ...
        </small>

    Roles are pre-numbered by FBref — (Referee) (AR1) (AR2) (4th) (VAR) — so the
    parser maps each to a fixed column without guessing AR order. Returns a
    single wide row; a role missing from the page (e.g. no VAR on some fixtures)
    stays NULL. ``None`` only when the Officials block is absent entirely
    (caller treats that as a parse failure, not as "no officials"), mirroring
    :func:`parse_match_managers`.

    Args:
        soup: BeautifulSoup of the match page.

    Returns:
        One-row DataFrame with columns ``referee``, ``ar1``, ``ar2``,
        ``fourth_official``, ``var`` (each a name or NULL). ``None`` if no
        Officials block is present.
    """
    # Locate the "Officials" label (a <small> inside <strong>), then its div.
    label = next(
        (sm for sm in soup.find_all('small')
         if sm.get_text(strip=True).lower() == 'officials'),
        None,
    )
    if label is None:
        return None
    block = label.find_parent('div')
    if block is None:
        return None

    cols = {c: None for c in _OFFICIAL_COLUMNS}
    for span in block.find_all('span'):
        text = _norm_name(span.get_text(' ', strip=True))
        if not text:
            continue
        m = _OFFICIAL_RE.match(text)
        if not m:
            continue
        name = _norm_name(m.group(1))
        col = _OFFICIAL_ROLE_TO_COL.get(m.group(2).lower())
        if col and name:
            cols[col] = name

    if all(v is None for v in cols.values()):
        return None
    return pd.DataFrame([cols])
