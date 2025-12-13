"""
FBref All Leagues parsing utilities for Airflow DAG

This module provides helper functions for the universal all-leagues DAG:
- Parsing all players (field + goalkeepers) for any team with dynamic paths
- Utility functions for data manipulation and reporting

Adapted from fbref_russian_premier_league_utils.py to work with any league.
"""

import os
import sys
from typing import Dict, List

# Add project root to path to import fbref_parser
# Use /opt/airflow for Docker, /root/data_platform for local testing
PROJECT_ROOT = "/opt/airflow" if os.path.exists("/opt/airflow/fbref_parser") else "/root/data_platform"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from fbref_parser import FieldPlayerParser, GoalkeeperParser
from fbref_parser.utils.file_helpers import normalize_name
from fbref_parser.constants import ALL_LEAGUES_DATA_DIR


def parse_team_all_players_universal(team_info: Dict) -> Dict:
    """
    Universal function to parse all players (field + goalkeepers) for any team

    This is an adapted version of parse_squad_all_players() from
    fbref_russian_premier_league_utils.py, but with dynamic paths based on league info.

    Args:
        team_info: Dict containing team and league information:
        {
            'team_name': 'Arsenal',
            'squad_url': 'https://fbref.com/en/squads/18bb7c10/...',
            'league_name': 'Premier League',
            'league_id': '9',
            'country': 'England',
            'league_tier': '1st'
        }

    Returns:
        Dict with parsing results:
        {
            'team': 'Arsenal',
            'league_name': 'Premier League',
            'country': 'England',
            'tier': '1st',
            'squad_url': '...',
            'field_players_count': 25,
            'goalkeepers_count': 3,
            'total_players': 28,
            'field_players_failed': 0,
            'goalkeepers_failed': 0,
            'output_dir': '/opt/airflow/data/leagues/england_premier_league/arsenal'
        }

    Raises:
        Exception: If squad parsing completely fails
    """
    team_name = team_info['team_name']
    squad_url = team_info['squad_url']
    league_name = team_info.get('league_name', 'Unknown League')
    country = team_info.get('country', 'Unknown')
    tier = team_info.get('league_tier', 'Unknown')

    print(f"\n{'='*80}")
    print(f"🏟️  ПАРСИНГ КОМАНДЫ: {team_name}")
    print(f"{'='*80}")
    print(f"Лига: {league_name} ({country})")
    print(f"Уровень: {tier}")
    print(f"URL: {squad_url}")

    # Create dynamic paths based on league and team
    league_dir_name = normalize_name(league_name)
    team_dir_name = normalize_name(team_name)

    team_base_dir = os.path.join(ALL_LEAGUES_DATA_DIR, league_dir_name, team_dir_name)
    field_players_dir = os.path.join(team_base_dir, "field_players")
    goalkeepers_dir = os.path.join(team_base_dir, "goalkeepers")

    os.makedirs(field_players_dir, exist_ok=True)
    os.makedirs(goalkeepers_dir, exist_ok=True)

    print(f"📁 Выходная директория: {team_base_dir}")
    print(f"   ⚽ Полевые игроки: {field_players_dir}")
    print(f"   🥅 Вратари: {goalkeepers_dir}")

    results = {
        "team": team_name,
        "league_name": league_name,
        "country": country,
        "tier": tier,
        "squad_url": squad_url,
        "field_players_count": 0,
        "goalkeepers_count": 0,
        "total_players": 0,
        "field_players_failed": 0,
        "goalkeepers_failed": 0,
        "output_dir": team_base_dir
    }

    try:
        # Override constants BEFORE creating parsers (to set output directories)
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
            # Parse squad (no limit, delay=6 seconds for rate limiting)
            field_count = field_parser.parse_squad(
                squad_url=squad_url,
                limit=None,
                delay=6  # 6 seconds to respect FBref rate limits
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
            # Parse squad goalkeepers
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

        # Treat "0 players" as a failure so the task can be retried with a fixed season URL
        if results['total_players'] == 0:
            results['field_players_failed'] += 1
            results['goalkeepers_failed'] += 1
            raise Exception("❌ На странице команды не найдено ни одного игрока (0 полевых, 0 вратарей)")

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
    print("🧪 Тестирование модуля fbref_all_leagues_utils")
    print("="*80)

    # Test: Parse a single team
    print("\nTEST 1: Парсинг одной команды (Arsenal)")
    print("="*80)

    try:
        test_team_info = {
            'team_name': 'Arsenal',
            'squad_url': 'https://fbref.com/en/squads/18bb7c10/Arsenal-Stats',
            'league_name': 'Premier League',
            'league_id': '9',
            'country': 'England',
            'league_tier': '1st'
        }

        print("\n⚠️  ВНИМАНИЕ: Это займет ~3-5 минут и сделает ~30 HTTP запросов")
        print("Для полного теста раскомментируйте следующую строку:")
        # results = parse_team_all_players_universal(test_team_info)
        # print(f"\n✅ Результаты парсинга: {results}")

        print("\n✅ Модуль готов к использованию")

    except Exception as e:
        print(f"\n❌ Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()
