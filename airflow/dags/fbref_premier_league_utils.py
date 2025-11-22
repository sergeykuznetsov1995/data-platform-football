"""
FBref Premier League parsing utilities for Airflow DAG

This module provides helper functions for parsing Premier League squad data:
- Extracting all Premier League team URLs from the league page
- Parsing both field players and goalkeepers for a given squad
- Handling file organization and error reporting

Required packages:
    - fbref_parser (field player and goalkeeper parsers)
    - beautifulsoup4 (HTML parsing)
    - cloudscraper (CloudFlare bypass)
"""

import os
import sys
from typing import Dict, List, Tuple
from bs4 import BeautifulSoup

# Add project root to path to import fbref_parser
PROJECT_ROOT = "/opt/airflow"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from fbref_parser import FieldPlayerParser, GoalkeeperParser
from fbref_parser.core.scraper import FBrefScraper
from fbref_parser.utils.file_helpers import normalize_name


# Constants
PREMIER_LEAGUE_URL = "https://fbref.com/en/comps/9/Premier-League-Stats"
PREMIER_LEAGUE_DATA_DIR = "/opt/airflow/data/premier_league"


def get_premier_league_squads() -> List[Dict[str, str]]:
    """
    Extract all Premier League team names and squad URLs from league page

    Parses the Premier League standings table to get links to all 20 teams.

    Returns:
        List of dicts with team_name and squad_url:
        [
            {"team_name": "Arsenal", "squad_url": "https://fbref.com/..."},
            {"team_name": "Manchester City", "squad_url": "https://..."},
            ...
        ]

    Raises:
        Exception: If unable to fetch or parse the Premier League page
    """
    print(f"🏴󠁧󠁢󠁥󠁮󠁧󠁿 Извлекаю список команд Premier League с: {PREMIER_LEAGUE_URL}")

    try:
        # Use FBrefScraper with rate limiting and CloudFlare bypass
        scraper = FBrefScraper()
        response = scraper.fetch_page(PREMIER_LEAGUE_URL)

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the league table - try multiple possible IDs
        league_table = None
        possible_table_ids = [
            'results2024-202591_overall',  # Current season format
            'results2024-2025_overall',
            'results_overall',
            'stats_squads_standard_for'  # Alternative: squad stats table
        ]

        for table_id in possible_table_ids:
            league_table = soup.find('table', {'id': table_id})
            if league_table:
                print(f"✅ Найдена таблица лиги с ID: {table_id}")
                break

        # If not found by ID, try to find by class or content
        if not league_table:
            all_tables = soup.find_all('table')
            for table in all_tables:
                # Check if table contains standings data
                headers = table.find_all('th')
                header_text = ' '.join([h.get_text().strip() for h in headers[:10]])
                if any(keyword in header_text.lower() for keyword in ['squad', 'mp', 'w', 'd', 'l', 'gf', 'ga', 'pts']):
                    league_table = table
                    print("✅ Найдена таблица лиги по содержимому заголовков")
                    break

        if not league_table:
            raise Exception("❌ Не найдена таблица турнирной таблицы Premier League")

        squads = []
        tbody = league_table.find('tbody')

        if not tbody:
            raise Exception("❌ Не найден tbody в таблице лиги")

        for row in tbody.find_all('tr'):
            # Skip header rows
            if 'thead' in row.get('class', []):
                continue

            cells = row.find_all(['td', 'th'])
            if len(cells) < 2:
                continue

            # Team name is usually in the second column (index 1) or first column with link
            team_cell = None
            for cell in cells[:3]:  # Check first 3 cells
                team_link = cell.find('a', href=lambda href: href and '/squads/' in href)
                if team_link:
                    team_cell = cell
                    break

            if not team_cell:
                continue

            team_link = team_cell.find('a', href=lambda href: href and '/squads/' in href)
            if team_link and team_link.get('href'):
                team_name = team_link.get_text(strip=True)
                squad_url = f"https://fbref.com{team_link['href']}"

                # Avoid duplicates
                if not any(s['team_name'] == team_name for s in squads):
                    squads.append({
                        'team_name': team_name,
                        'squad_url': squad_url
                    })

        print(f"\n✅ Найдено {len(squads)} команд Premier League:")
        for squad in squads:
            print(f"   - {squad['team_name']}")

        if len(squads) != 20:
            print(f"\n⚠️ ВНИМАНИЕ: Ожидалось 20 команд, но найдено {len(squads)}")

        return squads

    except Exception as e:
        print(f"❌ Ошибка при извлечении списка команд Premier League: {e}")
        raise


def parse_squad_all_players(squad_info: Dict[str, str]) -> Dict[str, any]:
    """
    Parse all players (field players + goalkeepers) for a given squad

    This function:
    1. Creates team-specific output directory
    2. Parses all field players using FieldPlayerParser
    3. Parses all goalkeepers using GoalkeeperParser
    4. Returns statistics about parsed players

    Args:
        squad_info: Dict with 'team_name' and 'squad_url' keys

    Returns:
        Dict with parsing results:
        {
            "team": "Arsenal",
            "squad_url": "https://...",
            "field_players_count": 25,
            "goalkeepers_count": 3,
            "total_players": 28,
            "field_players_failed": 0,
            "goalkeepers_failed": 0,
            "output_dir": "/root/data_platform/data/premier_league/Arsenal"
        }

    Raises:
        Exception: If squad parsing completely fails
    """
    team_name = squad_info['team_name']
    squad_url = squad_info['squad_url']

    print(f"\n{'='*80}")
    print(f"🏟️  ПАРСИНГ КОМАНДЫ: {team_name}")
    print(f"{'='*80}")
    print(f"🔗 URL: {squad_url}")

    # Create team-specific output directories for field players and goalkeepers
    team_dir_name = normalize_name(team_name)
    team_base_dir = os.path.join(PREMIER_LEAGUE_DATA_DIR, team_dir_name)
    field_players_dir = os.path.join(team_base_dir, "field_players")
    goalkeepers_dir = os.path.join(team_base_dir, "goalkeepers")
    os.makedirs(field_players_dir, exist_ok=True)
    os.makedirs(goalkeepers_dir, exist_ok=True)
    print(f"📁 Выходная директория: {team_base_dir}")
    print(f"   ⚽ Полевые игроки: {field_players_dir}")
    print(f"   🥅 Вратари: {goalkeepers_dir}")

    results = {
        "team": team_name,
        "squad_url": squad_url,
        "field_players_count": 0,
        "goalkeepers_count": 0,
        "total_players": 0,
        "field_players_failed": 0,
        "goalkeepers_failed": 0,
        "output_dir": team_base_dir
    }

    try:
        # Override constants BEFORE creating parsers
        from fbref_parser import constants
        original_const_field_dir = constants.DEFAULT_OUTPUT_DIR_FIELD_PLAYERS
        original_const_gk_dir = constants.DEFAULT_OUTPUT_DIR_GOALKEEPERS
        constants.DEFAULT_OUTPUT_DIR_FIELD_PLAYERS = field_players_dir
        constants.DEFAULT_OUTPUT_DIR_GOALKEEPERS = goalkeepers_dir

        # ========== PARSE FIELD PLAYERS ==========
        print(f"\n{'─'*80}")
        print("⚽ ПАРСИНГ ПОЛЕВЫХ ИГРОКОВ")
        print(f"{'─'*80}")

        try:
            field_parser = FieldPlayerParser()
            # Parse squad (no limit, default delay=4 seconds for rate limiting)
            field_count = field_parser.parse_squad(
                squad_url=squad_url,
                limit=None,
                delay=6  # 6 seconds to respect rate limits
            )
            results['field_players_count'] = field_count
            print(f"✅ Полевые игроки: {field_count} успешно спаршено")

        except Exception as e:
            print(f"❌ Ошибка при парсинге полевых игроков: {e}")
            results['field_players_failed'] = 1

        # ========== PARSE GOALKEEPERS ==========
        print(f"\n{'─'*80}")
        print("🥅 ПАРСИНГ ВРАТАРЕЙ")
        print(f"{'─'*80}")

        try:
            gk_parser = GoalkeeperParser()
            # Parse squad goalkeepers (метод не принимает limit и delay параметры)
            gk_count = gk_parser.parse_squad_goalkeepers(
                squad_url=squad_url
            )
            results['goalkeepers_count'] = gk_count
            print(f"✅ Вратари: {gk_count} успешно спаршено")

        except Exception as e:
            print(f"❌ Ошибка при парсинге вратарей: {e}")
            results['goalkeepers_failed'] = 1

        # Restore original directories
        constants.DEFAULT_OUTPUT_DIR_FIELD_PLAYERS = original_const_field_dir
        constants.DEFAULT_OUTPUT_DIR_GOALKEEPERS = original_const_gk_dir

        # Calculate totals
        results['total_players'] = results['field_players_count'] + results['goalkeepers_count']

        print(f"\n{'='*80}")
        print(f"✅ КОМАНДА {team_name} ЗАВЕРШЕНА")
        print(f"{'='*80}")
        print(f"⚽ Полевые игроки: {results['field_players_count']}")
        print(f"🥅 Вратари: {results['goalkeepers_count']}")
        print(f"👥 Всего игроков: {results['total_players']}")
        print(f"📁 Файлы сохранены в: {team_base_dir}")

        return results

    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА при парсинге команды {team_name}: {e}")
        results['error'] = str(e)
        raise


# For local testing
if __name__ == "__main__":
    print("🧪 Тестирование модуля fbref_premier_league_utils")

    # Test 1: Extract Premier League squads
    print("\n" + "="*80)
    print("TEST 1: Извлечение списка команд Premier League")
    print("="*80)

    try:
        squads = get_premier_league_squads()
        print(f"\n✅ Успешно извлечено {len(squads)} команд")

        # Test 2: Parse first squad (with limit for testing)
        if squads:
            print("\n" + "="*80)
            print("TEST 2: Парсинг первой команды (только для теста)")
            print("="*80)

            test_squad = squads[0]
            print(f"\nТестовая команда: {test_squad['team_name']}")
            print("⚠️ Для полного теста раскомментируйте строку ниже")
            # results = parse_squad_all_players(test_squad)
            # print(f"\n✅ Результаты парсинга: {results}")

    except Exception as e:
        print(f"\n❌ Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()
