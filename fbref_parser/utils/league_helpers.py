"""
League helpers for extracting team information from league pages

This module provides universal functions for extracting team/squad information
from any FBref league page. It's a parameterized version of the league-specific
extraction functions (like get_premier_league_squads).
"""

import logging
from typing import List, Dict
from bs4 import BeautifulSoup

from ..core.scraper import FBrefScraper
from ..constants import LEAGUE_TABLE_IDS

# Configure logging
logger = logging.getLogger(__name__)


def extract_league_teams(league_url: str, league_name: str = None, skip_on_error: bool = False) -> List[Dict[str, str]]:
    """
    Universal function to extract all teams from any FBref league page

    This is a parameterized version of get_premier_league_squads() from
    fbref_russian_premier_league_utils.py. Works for any league by trying
    multiple strategies to find the standings table.

    Args:
        league_url: URL of the league/season page
                    (e.g., 'https://fbref.com/en/comps/9/2024-2025/Premier-League-Stats')
        league_name: Name of the league for logging (optional)
        skip_on_error: If True, return empty list on error instead of raising exception

    Returns:
        List of team dictionaries:
        [{
            'team_name': 'Arsenal',
            'squad_url': 'https://fbref.com/en/squads/18bb7c10/Arsenal-Stats',
            'league_name': 'Premier League'  # if provided
        }, ...]

    Raises:
        Exception: If unable to fetch or parse the league page (unless skip_on_error=True)
    """
    display_name = league_name or league_url.split('/')[-1]

    print(f"\n{'='*80}")
    print(f"📊 ИЗВЛЕЧЕНИЕ КОМАНД ЛИГИ: {display_name}")
    print(f"{'='*80}")
    print(f"URL: {league_url}")

    # Validate URL format before processing
    if '/comps/' in league_url:
        parts = league_url.split('/comps/')[1].split('/')
        if parts:
            potential_id = parts[0]
            if not potential_id.isdigit():
                error_msg = f"Invalid league URL format: {league_url} (league ID must be numeric, got: {potential_id})"
                print(f"❌ {error_msg}")
                logger.error(error_msg)
                if skip_on_error:
                    return []
                else:
                    raise ValueError(error_msg)

    try:
        # Fetch league page with rate limiting
        scraper = FBrefScraper()
        response = scraper.fetch_page(league_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Strategy 1: Try to find table by ID patterns
        league_table = None

        # Extract season and league ID from URL for table ID matching
        # URL format: /en/comps/9/2024-2025/Premier-League-Stats
        season = None
        league_id = None
        if '/comps/' in league_url:
            parts = league_url.split('/comps/')[1].split('/')
            if len(parts) >= 2:
                league_id = parts[0]
                if len(parts) >= 3:
                    # Check if second part looks like a season
                    if '-' in parts[1] and any(c.isdigit() for c in parts[1]):
                        season = parts[1].replace('-', '')  # "2024-2025" -> "20242025"

        # Try multiple table ID patterns
        possible_table_ids = []
        if season and league_id:
            possible_table_ids.append(f'results{season}{league_id}1_overall')
            possible_table_ids.append(f'results{season}{league_id}_overall')
        if season:
            possible_table_ids.append(f'results{season}_overall')
        if league_id:
            possible_table_ids.append(f'results{league_id}_overall')
        possible_table_ids.extend(['results_overall', 'stats_squads_standard_for'])

        for table_id in possible_table_ids:
            league_table = soup.find('table', {'id': table_id})
            if league_table:
                print(f"✅ Таблица найдена по ID: {table_id}")
                break

        # Strategy 2: Search by table content (fallback)
        if not league_table:
            print("⚠️  Таблица не найдена по ID, поиск по содержимому...")
            all_tables = soup.find_all('table')

            for table in all_tables:
                # Check if table contains standings data
                headers = table.find_all('th')
                header_text = ' '.join([h.get_text().strip().lower() for h in headers[:15]])

                # Look for typical standings column headers
                standings_keywords = ['squad', 'mp', 'w', 'd', 'l', 'gf', 'ga', 'gd', 'pts', 'matches played']
                matches = sum(1 for keyword in standings_keywords if keyword in header_text)

                if matches >= 5:  # At least 5 keywords match
                    league_table = table
                    print(f"✅ Таблица найдена по содержимому (совпадений: {matches})")
                    break

        if not league_table:
            if skip_on_error:
                print("❌ Не найдена таблица турнирной таблицы на странице лиги (skip_on_error=True, возвращаю пустой список)")
                return []
            raise Exception("❌ Не найдена таблица турнирной таблицы на странице лиги")

        # Extract team links from table
        tbody = league_table.find('tbody')
        if not tbody:
            raise Exception("❌ Не найден tbody в таблице лиги")

        teams = []
        for row in tbody.find_all('tr'):
            # Skip header rows within tbody
            if 'thead' in row.get('class', []):
                continue

            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            # Find team cell (contains link to /squads/)
            team_cell = None
            for cell in cells[:4]:  # Check first 4 columns
                team_link = cell.find('a', href=lambda href: href and '/squads/' in href)
                if team_link:
                    team_cell = cell
                    break

            if not team_cell:
                continue

            team_link = team_cell.find('a', href=lambda href: href and '/squads/' in href)
            if team_link and team_link.get('href'):
                team_name = team_link.get_text(strip=True)
                squad_url = team_link['href']

                # Build full URL
                if not squad_url.startswith('http'):
                    squad_url = f"https://fbref.com{squad_url}"

                # Create team dict
                team_data = {
                    'team_name': team_name,
                    'squad_url': squad_url
                }

                # Add league name if provided
                if league_name:
                    team_data['league_name'] = league_name

                # Avoid duplicates
                if not any(t['team_name'] == team_name for t in teams):
                    teams.append(team_data)

        print(f"\n✅ НАЙДЕНО КОМАНД: {len(teams)}")
        for i, team in enumerate(teams, 1):
            print(f"   {i}. {team['team_name']}")

        if len(teams) == 0:
            if skip_on_error:
                print("❌ Не найдено ни одной команды в таблице (skip_on_error=True, возвращаю пустой список)")
                return []
            raise Exception("❌ Не найдено ни одной команды в таблице")

        return teams

    except Exception as e:
        print(f"❌ Ошибка при извлечении команд лиги {display_name}: {e}")
        logger.error(f"Error extracting teams for {display_name}: {e}", exc_info=True)
        if skip_on_error:
            print(f"⚠️  Пропуск лиги {display_name} из-за ошибки (skip_on_error=True)")
            return []
        else:
            raise


# For local testing
if __name__ == "__main__":
    print("🧪 Тестирование модуля league_helpers")
    print("="*80)

    # Test 1: Extract teams from Premier League
    print("\nTEST 1: Извлечение команд Premier League")
    print("="*80)

    try:
        epl_url = "https://fbref.com/en/comps/9/Premier-League-Stats"
        teams = extract_league_teams(epl_url, league_name="Premier League")

        print(f"\n✅ Успешно извлечено {len(teams)} команд")
        print("\nПримеры (первые 5 команд):")
        for team in teams[:5]:
            print(f"\n{team['team_name']}")
            print(f"  URL: {team['squad_url']}")
            print(f"  Лига: {team.get('league_name', 'N/A')}")

    except Exception as e:
        print(f"\n❌ Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()

    # Test 2: Extract teams from a smaller league (MLS)
    print("\n" + "="*80)
    print("TEST 2: Извлечение команд MLS (Major League Soccer)")
    print("="*80)

    try:
        mls_url = "https://fbref.com/en/comps/22/Major-League-Soccer-Stats"
        teams = extract_league_teams(mls_url, league_name="MLS")

        print(f"\n✅ Успешно извлечено {len(teams)} команд")
        print("\nПримеры (первые 5 команд):")
        for team in teams[:5]:
            print(f"\n{team['team_name']}")
            print(f"  URL: {team['squad_url']}")

    except Exception as e:
        print(f"\n❌ Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()
