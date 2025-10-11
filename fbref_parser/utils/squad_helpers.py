"""
Squad parsing utilities for FBref.com

This module provides functions for extracting player links from squad pages:
- Field player link extraction
- Goalkeeper link extraction
"""

import requests
from bs4 import BeautifulSoup
import re
from typing import List, Tuple

from ..constants import DEFAULT_HEADERS


def extract_field_player_links(squad_url: str) -> List[Tuple[str, str]]:
    """
    Extract links to all field players from squad page

    Parses the squad statistics table and extracts player URLs,
    excluding goalkeepers (GK position).

    Args:
        squad_url: URL of the squad page

    Returns:
        List of tuples: [(player_name, player_url), ...]
    """
    print(f"🔍 Извлекаю ссылки на полевых игроков с: {squad_url}")

    try:
        response = requests.get(squad_url, headers=DEFAULT_HEADERS)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all tables on page
        all_tables = soup.find_all('table')

        # Find standard stats table
        standard_stats_table = soup.find('table', {'id': 'all_stats_standard'})

        # If not found by exact ID, try alternative variants
        if not standard_stats_table:
            # Try other possible IDs - start with most likely
            alternative_ids = ['stats_standard_9', 'stats_standard', 'stats_standard_combined']
            for alt_id in alternative_ids:
                standard_stats_table = soup.find('table', {'id': alt_id})
                if standard_stats_table:
                    break

            # If still not found, try search by header content
            if not standard_stats_table:
                for table in all_tables:
                    headers = table.find_all(['th', 'td'])
                    header_text = ' '.join([h.get_text().strip() for h in headers[:10]])
                    if any(keyword in header_text.lower() for keyword in ['player', 'nation', 'pos', 'age', 'mp', 'starts']):
                        standard_stats_table = table
                        break

        if not standard_stats_table:
            print("❌ Не найдена таблица стандартной статистики")
            return []

        player_links = []

        # Try finding field players in all standard stats tables
        tables_to_check = [standard_stats_table]

        # If main table has only goalkeepers, check ALL tables
        # Add all tables for checking
        for table in all_tables:
            table_id = table.get('id', '')
            if table_id and table_id not in [t.get('id', '') for t in tables_to_check]:
                if any(keyword in table_id for keyword in ['stats_', 'standard', 'shooting', 'passing', 'defense']):
                    tables_to_check.append(table)

        for table_idx, table in enumerate(tables_to_check):
            # Extract player rows from tbody
            tbody = table.find('tbody')
            if not tbody:
                continue

            found_in_table = 0

            for row in tbody.find_all('tr'):
                # Skip header rows
                if 'thead' in row.get('class', []):
                    continue

                cells = row.find_all(['td', 'th'])
                if len(cells) < 4:  # Minimum columns: Player, Nation, Position, Age
                    continue

                # First cell contains player name and link
                player_cell = cells[0]

                # Position usually in 3rd column (index 2)
                position_cell = cells[2] if len(cells) > 2 else None
                position = position_cell.get_text(strip=True) if position_cell else ""

                # Skip goalkeepers
                if 'GK' in position.upper():
                    continue

                # Find player link
                player_link = player_cell.find('a')
                if player_link and player_link.get('href'):
                    href = player_link.get('href')

                    # Check that this is a player page link
                    if '/players/' in href:
                        player_name = player_cell.get_text(strip=True)

                        # Check for duplicates
                        if any(existing_name == player_name for existing_name, _ in player_links):
                            continue

                        full_url = f"https://fbref.com{href}"

                        # Convert to all_comps URL
                        if not '/all_comps/' in full_url:
                            # Replace regular URL with all_comps URL
                            parts = href.split('/')
                            if len(parts) >= 4:
                                player_id = parts[3]
                                player_url_name = parts[4] if len(parts) > 4 else player_name.replace(' ', '-')
                                full_url = f"https://fbref.com/en/players/{player_id}/all_comps/{player_url_name}-Stats---All-Competitions"

                        player_links.append((player_name, full_url))
                        found_in_table += 1

            # If found enough players in this table, can stop
            if len(player_links) >= 25:  # Limit to reasonable number
                break

        print(f"\n📊 Найдено {len(player_links)} полевых игроков")
        return player_links

    except Exception as e:
        print(f"❌ Ошибка при извлечении ссылок на игроков: {e}")
        return []


def extract_goalkeeper_links(squad_url: str) -> List[Tuple[str, str]]:
    """
    Extract links to all goalkeepers from squad page

    Parses the squad statistics table and extracts goalkeeper URLs
    (players with GK position only).

    Args:
        squad_url: URL of the squad page

    Returns:
        List of tuples: [(goalkeeper_name, goalkeeper_url), ...]
    """
    print(f"🥅 Извлекаю ссылки на вратарей с: {squad_url}")

    try:
        response = requests.get(squad_url, headers=DEFAULT_HEADERS)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find standard stats table
        standard_stats_table = soup.find('table', {'id': 'stats_standard_9'})
        if not standard_stats_table:
            standard_stats_table = soup.find('table', {'id': 'stats_standard_combined'})

        # If not found by exact ID, try alternative variants
        if not standard_stats_table:
            all_tables = soup.find_all('table')
            alternative_ids = ['all_stats_standard', 'stats_standard']
            for alt_id in alternative_ids:
                standard_stats_table = soup.find('table', {'id': alt_id})
                if standard_stats_table:
                    break

            # If still not found, try search by header content
            if not standard_stats_table:
                for table in all_tables:
                    headers = table.find_all(['th', 'td'])
                    header_text = ' '.join([h.get_text().strip() for h in headers[:10]])
                    if any(keyword in header_text.lower() for keyword in ['player', 'nation', 'pos', 'age', 'mp', 'starts']):
                        standard_stats_table = table
                        break

        if not standard_stats_table:
            print("❌ Не найдена таблица стандартной статистики")
            return []

        goalkeeper_links = []

        # Extract player rows from tbody
        tbody = standard_stats_table.find('tbody')
        if not tbody:
            print("❌ Не найден tbody в таблице")
            return []

        for row in tbody.find_all('tr'):
            # Skip header rows
            if 'thead' in row.get('class', []):
                continue

            cells = row.find_all(['td', 'th'])
            if len(cells) < 4:  # Minimum columns: Player, Nation, Position, Age
                continue

            # First cell contains player name and link
            player_cell = cells[0]

            # Position usually in 3rd column (index 2)
            position_cell = cells[2] if len(cells) > 2 else None
            position = position_cell.get_text(strip=True) if position_cell else ""

            # Keep ONLY goalkeepers
            if 'GK' not in position.upper():
                continue

            # Find player link
            player_link = player_cell.find('a')
            if player_link and player_link.get('href'):
                href = player_link.get('href')

                # Check that this is a player page link
                if '/players/' in href:
                    player_name = player_cell.get_text(strip=True)

                    # Check for duplicates
                    if any(existing_name == player_name for existing_name, _ in goalkeeper_links):
                        continue

                    # Convert to all_comps URL
                    if '/all_comps/' not in href:
                        # Replace part of URL to get all competitions stats
                        href = re.sub(r'(/players/[^/]+/)\d{4}-\d{4}/', r'\1all_comps/', href)
                        href = re.sub(r'/[^/]*-Stats$', r'Stats---All-Competitions', href)
                        if not href.endswith('Stats---All-Competitions'):
                            # If replacement didn't work, rebuild URL
                            player_id = href.split('/players/')[1].split('/')[0]
                            normalized_name = player_name.replace(' ', '-')
                            href = f"/en/players/{player_id}/all_comps/{normalized_name}-Stats---All-Competitions"

                    full_url = f"https://fbref.com{href}"
                    goalkeeper_links.append((player_name, full_url))

        print(f"✅ Найдено {len(goalkeeper_links)} вратарей:")
        for name, _ in goalkeeper_links:
            print(f"   - {name}")

        return goalkeeper_links

    except Exception as e:
        print(f"❌ Ошибка при извлечении ссылок на вратарей: {e}")
        return []
