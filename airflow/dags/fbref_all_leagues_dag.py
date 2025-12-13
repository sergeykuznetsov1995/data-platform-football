"""
FBref All Leagues Universal Parser DAG

This DAG automatically discovers and parses all football leagues from FBref.com:
- Discovers all 1st, 2nd, and 3rd tier men's leagues automatically
- Extracts teams from each league
- Parses all field players and goalkeepers
- Implements strict rate limiting (10 req/min via pool)
- Supports caching of league metadata for faster re-runs

Expected execution time: ~105 hours (4.4 days) for first run
Subsequent runs: ~10-15 hours (idempotent - skips existing files)

Usage:
1. Create Airflow pool: Admin → Pools → Add
   Pool: fbref_pool
   Slots: 1
   Description: Rate limiting for FBref scraping

2. Trigger DAG: airflow dags trigger fbref_all_leagues_parser
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict

from airflow.decorators import dag, task

# Add project root to path
PROJECT_ROOT = "/opt/airflow" if os.path.exists("/opt/airflow/fbref_parser") else "/root/data_platform"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@dag(
    dag_id='fbref_all_leagues_parser',
    description='Universal parser for all FBref leagues (1st, 2nd, 3rd tier) with automatic discovery',
    schedule=None,  # Manual trigger only (can be changed to weekly schedule)
    start_date=datetime(2025, 1, 27),
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(hours=120)  # 5 days maximum
    },
    tags=['fbref', 'all_leagues', 'football', 'parsing']
)
def fbref_all_leagues_parser():
    """
    Universal DAG for parsing all FBref football leagues

    Workflow:
    1. discover_all_leagues() - Extract ~100 leagues from /en/comps/
    2. extract_league_teams() - Get teams for each league (dynamic mapping)
    3. flatten_team_lists() - Combine all teams into single list
    4. parse_team_players() - Parse each team (field + GK) via pool
    5. group_results_by_league() - Group results for reporting
    5.5. prepare_league_reports_data() - Prepare data for dynamic mapping
    6. report_league_results() - Report per league (dynamic mapping)
    7. report_final_results() - Final summary report
    """

    @task
    def discover_all_leagues() -> List[Dict]:
        """
        Discover all leagues (1st, 2nd, 3rd tier) from FBref /en/comps/

        Returns:
            List of league metadata dictionaries:
            [{
                'league_id': '9',
                'name': 'Premier League',
                'country': 'England',
                'tier': '1st',
                'gender': 'M',
                'season_url': 'https://fbref.com/en/comps/9/2024-2025/...'
            }, ...]
        """
        from fbref_parser.utils.league_discovery import discover_all_leagues

        print(f"\n{'#'*80}")
        print(f"🔍 ЗАДАЧА 1: ОБНАРУЖЕНИЕ ВСЕХ ЛИГ")
        print(f"{'#'*80}")

        # Use cache if available (speeds up re-runs)
        leagues = discover_all_leagues(
            use_cache=True,
            tiers=['1st', '2nd', '3rd'],
            gender='M'
        )

        print(f"\n✅ Обнаружено {len(leagues)} мужских лиг (1st, 2nd, 3rd tier)")

        # Log tier distribution
        tier_counts = {}
        for league in leagues:
            tier = league['tier']
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        for tier in ['1st', '2nd', '3rd']:
            count = tier_counts.get(tier, 0)
            print(f"   {tier} tier: {count} лиг")

        return leagues

    @task(pool='fbref_pool')  # Rate limiting: 1 slot = sequential execution
    def extract_league_teams(league_info: Dict) -> List[Dict]:
        """
        Extract all teams from a single league

        Args:
            league_info: League metadata dict with 'season_url', 'name', etc.

        Returns:
            List of team dicts with added league context:
            [{
                'team_name': 'Arsenal',
                'squad_url': 'https://...',
                'league_name': 'Premier League',
                'league_id': '9',
                'league_tier': '1st',
                'country': 'England'
            }, ...]
        """
        from fbref_parser.utils.league_helpers import extract_league_teams

        league_name = league_info['name']
        season_url = league_info['season_url']

        print(f"\n{'#'*80}")
        print(f"📊 ЗАДАЧА 2: ИЗВЛЕЧЕНИЕ КОМАНД - {league_name}")
        print(f"{'#'*80}")

        teams = extract_league_teams(
            league_url=season_url,
            league_name=league_name,
            skip_on_error=True  # Continue DAG even if some leagues fail
        )

        # Check if league extraction failed (returned empty list)
        if not teams:
            print(f"⚠️  ВНИМАНИЕ: Лига {league_name} пропущена (не найдены команды)")
            print(f"   Возможные причины:")
            print(f"   - Неправильный формат URL: {season_url}")
            print(f"   - Страница лиги не содержит турнирную таблицу")
            print(f"   - Проблемы с доступом к странице")
            return []

        # Add league context to each team
        for team in teams:
            team['league_name'] = league_info['name']
            team['league_id'] = league_info['league_id']
            team['league_tier'] = league_info['tier']
            team['country'] = league_info['country']

        print(f"✅ Извлечено {len(teams)} команд из {league_name}")

        return teams

    @task
    def flatten_team_lists(all_teams: List[List[Dict]]) -> List[Dict]:
        """
        Flatten list of team lists into single list

        Args:
            all_teams: [[team1, team2], [team3, team4], ...]

        Returns:
            Flattened list: [team1, team2, team3, team4, ...]
        """
        print(f"\n{'#'*80}")
        print(f"🔗 ЗАДАЧА 3: ОБЪЕДИНЕНИЕ СПИСКОВ КОМАНД")
        print(f"{'#'*80}")

        flattened = []
        for league_teams in all_teams:
            if league_teams:  # Skip empty lists
                flattened.extend(league_teams)

        print(f"✅ Всего команд для парсинга: {len(flattened)}")

        # Log breakdown by tier
        tier_counts = {}
        for team in flattened:
            tier = team.get('league_tier', 'Unknown')
            tier_counts[tier] = tier_counts.get(tier, 0) + 1

        for tier, count in sorted(tier_counts.items()):
            print(f"   {tier}: {count} команд")

        return flattened

    @task
    def chunk_teams(teams: List[Dict], chunk_size: int = 300) -> List[List[Dict]]:
        """
        Split teams list into chunks to stay under Airflow dynamic task mapping limit.

        Args:
            teams: Flat list of teams
            chunk_size: Max teams per batch

        Returns:
            List of team batches
        """
        batches = [teams[i:i + chunk_size] for i in range(0, len(teams), chunk_size)]
        print(f"✅ Команды разбиты на {len(batches)} батчей (по {chunk_size} шт.)")
        return batches

    @task(pool='fbref_pool', retries=2, retry_delay=timedelta(minutes=10))
    def parse_team_players_batch(team_batch: List[Dict]) -> List[Dict]:
        """
        Parse players for a batch of teams sequentially inside one mapped task.

        This reduces the number of mapped tasks to avoid Airflow's 1024 mapping limit.
        """
        from fbref_all_leagues_utils import parse_team_all_players_universal

        results: List[Dict] = []
        for team_info in team_batch:
            team_name = team_info.get('team_name', 'Unknown')
            league_name = team_info.get('league_name', 'Unknown')

            print(f"\n{'#'*80}")
            print(f"⚽ ПАРСИНГ КОМАНДЫ (batch): {team_name} ({league_name})")
            print(f"{'#'*80}")

            try:
                res = parse_team_all_players_universal(team_info)
                print(f"✅ {team_name}: {res.get('total_players', 0)} игроков спаршено")
                results.append(res)
            except Exception as e:
                print(f"❌ Ошибка при парсинге команды {team_name}: {e}")
                results.append({
                    'team': team_name,
                    'league_name': league_name,
                    'error': str(e),
                    'field_players_failed': 1,
                    'goalkeepers_failed': 1,
                    'total_players': 0
                })

        return results

    @task
    def flatten_batch_results(batch_results: List[List[Dict]]) -> List[Dict]:
        """
        Flatten list of batch results into a single list of team results.
        """
        flattened: List[Dict] = []
        for batch in batch_results:
            if batch:
                flattened.extend(batch)
        print(f"✅ Всего результатов команд: {len(flattened)}")
        return flattened

    @task
    def group_results_by_league(team_results: List[Dict]) -> Dict[str, List]:
        """
        Group team parsing results by league for reporting

        Args:
            team_results: List of team result dicts

        Returns:
            Dict mapping league names to team results:
            {
                'Premier League': [team1_results, team2_results, ...],
                'La Liga': [team1_results, team2_results, ...],
                ...
            }
        """
        print(f"\n{'#'*80}")
        print(f"📊 ЗАДАЧА 5: ГРУППИРОВКА РЕЗУЛЬТАТОВ ПО ЛИГАМ")
        print(f"{'#'*80}")

        leagues = {}
        for result in team_results:
            league = result.get('league_name', 'Unknown League')
            if league not in leagues:
                leagues[league] = []
            leagues[league].append(result)

        print(f"✅ Результаты сгруппированы по {len(leagues)} лигам")

        return leagues

    @task
    def prepare_league_reports_data(grouped: Dict[str, List]) -> List[Dict]:
        """
        Prepare league data for dynamic task mapping

        This task converts the grouped dict into a list of dicts
        suitable for expand() operator.

        Args:
            grouped: Dict mapping league names to team results

        Returns:
            List of dicts: [{'league_name': ..., 'teams': [...]}, ...]
        """
        print(f"\n{'#'*80}")
        print(f"🔄 ЗАДАЧА 5.5: ПОДГОТОВКА ДАННЫХ ДЛЯ ОТЧЕТОВ")
        print(f"{'#'*80}")

        league_data_list = [
            {'league_name': league_name, 'teams': teams}
            for league_name, teams in grouped.items()
        ]

        print(f"✅ Подготовлено {len(league_data_list)} записей для отчетов")

        return league_data_list

    @task
    def report_league_results(league_data: Dict) -> Dict:
        """
        Generate report for a single league

        Args:
            league_data: Dict with 'league_name' and 'teams' keys

        Returns:
            League summary dict
        """
        league_name = league_data['league_name']
        teams = league_data['teams']

        total_field = sum(t.get('field_players_count', 0) for t in teams)
        total_gk = sum(t.get('goalkeepers_count', 0) for t in teams)
        total_errors = sum(
            t.get('field_players_failed', 0) + t.get('goalkeepers_failed', 0)
            for t in teams
        )

        print(f"\n{'='*80}")
        print(f"📊 ОТЧЕТ ПО ЛИГЕ: {league_name}")
        print(f"{'='*80}")
        print(f"Команд: {len(teams)}")
        print(f"Полевых игроков: {total_field}")
        print(f"Вратарей: {total_gk}")
        print(f"Всего игроков: {total_field + total_gk}")
        print(f"Ошибок: {total_errors}")

        return {
            'league': league_name,
            'teams_count': len(teams),
            'field_players': total_field,
            'goalkeepers': total_gk,
            'total_players': total_field + total_gk,
            'errors': total_errors
        }

    @task
    def report_final_results(league_reports: List[Dict]):
        """
        Generate final summary report across all leagues

        Args:
            league_reports: List of league summary dicts
        """
        # Filter out empty leagues (those that were skipped)
        successful_leagues = [r for r in league_reports if r['teams_count'] > 0]
        skipped_leagues = [r for r in league_reports if r['teams_count'] == 0]

        total_leagues = len(successful_leagues)
        total_skipped = len(skipped_leagues)
        total_teams = sum(r['teams_count'] for r in successful_leagues)
        total_players = sum(r['total_players'] for r in successful_leagues)
        total_errors = sum(r['errors'] for r in successful_leagues)

        print(f"\n{'#'*80}")
        print(f"🎯 ФИНАЛЬНЫЙ ОТЧЕТ: ВСЕ ЛИГИ")
        print(f"{'#'*80}")
        print(f"Успешных лиг: {total_leagues}")
        if total_skipped > 0:
            print(f"Пропущенных лиг: {total_skipped} ⚠️")
        print(f"Команд: {total_teams}")
        print(f"Игроков: {total_players}")
        print(f"Ошибок парсинга: {total_errors}")

        if total_players > 0:
            success_rate = (1 - total_errors / max(total_players, 1)) * 100
            print(f"Успешность: {success_rate:.1f}%")

        # Top 10 leagues by player count
        if successful_leagues:
            sorted_leagues = sorted(successful_leagues, key=lambda x: x['total_players'], reverse=True)
            print(f"\n📈 ТОП-10 лиг по количеству игроков:")
            for i, league in enumerate(sorted_leagues[:10], 1):
                print(f"   {i}. {league['league']}: {league['total_players']} игроков")

        # Report skipped leagues
        if skipped_leagues:
            print(f"\n⚠️  ПРОПУЩЕННЫЕ ЛИГИ ({len(skipped_leagues)}):")
            for league in skipped_leagues:
                print(f"   - {league['league']}")

        print(f"\n{'#'*80}")
        print(f"✅ ПАРСИНГ ВСЕХ ЛИГ ЗАВЕРШЕН")
        print(f"{'#'*80}")

    # ========== DAG WORKFLOW DEFINITION ==========
    # Define task dependencies and data flow

    # Step 1: Discover all leagues
    leagues = discover_all_leagues()

    # Step 2: Extract teams for each league (dynamic task mapping)
    # Creates ~100 tasks (one per league), executed sequentially via pool
    all_teams = extract_league_teams.expand(league_info=leagues)

    # Step 3: Flatten team lists
    flattened_teams = flatten_team_lists(all_teams)

    # Step 3.5: Chunk teams to avoid >1024 mapped tasks
    team_batches = chunk_teams(flattened_teams)

    # Step 4: Parse players for each team (dynamic task mapping)
    # Now mapped by batches to stay under mapping limits; inside each batch teams parsed sequentially
    batch_results = parse_team_players_batch.expand(team_batch=team_batches)

    # Flatten batch results back to per-team results
    team_results = flatten_batch_results(batch_results)

    # Step 5: Group results by league
    grouped = group_results_by_league(team_results)

    # Step 5.5: Prepare data for dynamic league reports
    league_reports_data = prepare_league_reports_data(grouped)

    # Step 6: Generate league reports (dynamic task mapping)
    # Creates ~100 tasks (one per league)
    league_reports = report_league_results.expand(league_data=league_reports_data)

    # Step 7: Generate final report
    report_final_results(league_reports)


# Instantiate the DAG
fbref_all_leagues_dag = fbref_all_leagues_parser()
