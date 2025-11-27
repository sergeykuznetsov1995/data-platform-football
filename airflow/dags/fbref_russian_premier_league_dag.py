"""
Airflow DAG for parsing all Russian Premier League players from FBref.com

This DAG orchestrates the complete parsing of all 16 Russian Premier League teams,
including both field players and goalkeepers for the 2024-2025 season.

Schedule: Manual trigger only (schedule=None)
Expected runtime: ~50 minutes for full parsing of ~400 players

Architecture:
    1. extract_premier_league_squads: Fetch list of all 16 teams
    2. parse_squad_players (dynamic): Parse each team in parallel
    3. report_results: Aggregate and display final statistics

Rate Limiting:
    - Uses 'fbref_pool' (1 slot) to serialize requests across tasks
    - Parsers use 6-8 second delays between individual player requests
    - Complies with FBref's 10 requests/minute limit

Output Structure:
    /root/data_platform/data/russian_premier_league/
        zenit/
            player_name.csv
            ...
        spartak_moscow/
            player_name.csv
            ...
        ...

Usage:
    1. Create 'fbref_pool' in Airflow UI (Admin -> Pools):
       - Pool: fbref_pool
       - Slots: 1
       - Description: Rate limiting for FBref scraping
    2. Trigger DAG manually via UI or CLI
    3. Monitor progress in task logs
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from typing import Dict, List
import logging

# Import utility functions from same directory
from fbref_russian_premier_league_utils import get_premier_league_squads, parse_squad_all_players


# DAG default arguments
default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),  # Maximum 2 hours for safety
}


@dag(
    dag_id='fbref_russian_premier_league_parser',
    default_args=default_args,
    description='Parse all Russian Premier League 2024-2025 players (field + goalkeepers) from FBref.com',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 11, 20),
    catchup=False,
    max_active_runs=1,  # Only one instance at a time
    tags=['fbref', 'russian_premier_league', 'scraping', 'football'],
    doc_md=__doc__,
)
def fbref_premier_league_parser():
    """
    Main DAG for parsing Russian Premier League player statistics from FBref.com

    This DAG implements a robust, rate-limited scraping pipeline that:
    - Extracts all 16 Russian Premier League team URLs
    - Dynamically creates parsing tasks for each team
    - Parses both field players and goalkeepers
    - Aggregates results and reports statistics
    """

    @task(
        task_id='extract_premier_league_squads',
        doc_md="""
        Extract list of all Russian Premier League teams and their squad URLs

        This task scrapes the Russian Premier League standings page to get links
        to all 16 teams' squad pages. Uses FBrefScraper with CloudFlare
        bypass and rate limiting.

        Returns:
            List[Dict]: List of dicts with 'team_name' and 'squad_url' keys
        """,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )
    def extract_premier_league_squads() -> List[Dict[str, str]]:
        """
        Fetch all Russian Premier League team URLs from league page

        Returns:
            List of 16 dicts: [{"team_name": "Zenit", "squad_url": "https://..."}, ...]
        """
        logging.info("="*80)
        logging.info("TASK: Extract Russian Premier League Squads")
        logging.info("="*80)

        try:
            squads = get_premier_league_squads()

            logging.info(f"\n✅ Successfully extracted {len(squads)} Russian Premier League teams")

            if len(squads) != 16:
                logging.warning(f"⚠️ Expected 16 teams but found {len(squads)}")

            # Log team names for verification
            logging.info("\n📋 Teams to be parsed:")
            for i, squad in enumerate(squads, 1):
                logging.info(f"   {i:2d}. {squad['team_name']}")

            return squads

        except Exception as e:
            logging.error(f"❌ Failed to extract Russian Premier League squads: {e}")
            raise

    @task(
        task_id='parse_squad_players',
        doc_md="""
        Parse all players (field + goalkeepers) for a single squad

        This task uses dynamic task mapping to create one instance per team.
        Each instance parses all players for that team and saves CSVs to
        team-specific directory.

        Uses 'fbref_pool' to ensure only one parsing task runs at a time,
        preventing rate limit violations.

        Args:
            squad_info: Dict with 'team_name' and 'squad_url' keys

        Returns:
            Dict: Parsing statistics (field_players_count, goalkeepers_count, etc.)
        """,
        retries=2,
        retry_delay=timedelta(minutes=5),
        pool='fbref_pool',  # Critical: Rate limiting via single-slot pool
        execution_timeout=timedelta(hours=1),  # Max 1 hour per team
    )
    def parse_squad_players(squad_info: Dict[str, str]) -> Dict[str, any]:
        """
        Parse all players for a given squad

        Args:
            squad_info: Dict containing 'team_name' and 'squad_url'

        Returns:
            Dict with parsing results and statistics
        """
        team_name = squad_info['team_name']

        logging.info("\n" + "="*80)
        logging.info(f"TASK: Parse Squad Players - {team_name}")
        logging.info("="*80)

        try:
            results = parse_squad_all_players(squad_info)

            # Log summary
            logging.info(f"\n✅ {team_name} parsing completed:")
            logging.info(f"   ⚽ Field players: {results['field_players_count']}")
            logging.info(f"   🥅 Goalkeepers: {results['goalkeepers_count']}")
            logging.info(f"   👥 Total: {results['total_players']}")
            logging.info(f"   📁 Output: {results['output_dir']}")

            if results.get('field_players_failed') or results.get('goalkeepers_failed'):
                logging.warning(f"⚠️ Some parsing errors occurred for {team_name}")

            return results

        except Exception as e:
            logging.error(f"❌ Failed to parse {team_name}: {e}")
            # Return error result instead of raising to allow other teams to continue
            return {
                "team": team_name,
                "squad_url": squad_info['squad_url'],
                "field_players_count": 0,
                "goalkeepers_count": 0,
                "total_players": 0,
                "error": str(e)
            }

    @task(
        task_id='report_results',
        doc_md="""
        Aggregate parsing results and generate final report

        Collects results from all parse_squad_players tasks and generates
        comprehensive statistics about the parsing run.

        Args:
            results_list: List of result dicts from parse_squad_players tasks

        Returns:
            Dict: Aggregated statistics and summary
        """,
    )
    def report_results(results_list: List[Dict[str, any]]) -> Dict[str, any]:
        """
        Generate final parsing report

        Args:
            results_list: List of result dicts from all team parsing tasks

        Returns:
            Dict with aggregated statistics
        """
        logging.info("\n" + "="*80)
        logging.info("TASK: Report Results")
        logging.info("="*80)

        # Aggregate statistics
        total_teams = len(results_list)
        total_field_players = sum(r.get('field_players_count', 0) for r in results_list)
        total_goalkeepers = sum(r.get('goalkeepers_count', 0) for r in results_list)
        total_players = total_field_players + total_goalkeepers

        teams_with_errors = [r['team'] for r in results_list if r.get('error')]
        successful_teams = [r['team'] for r in results_list if not r.get('error')]

        # Build summary report
        summary = {
            "total_teams": total_teams,
            "successful_teams": len(successful_teams),
            "failed_teams": len(teams_with_errors),
            "total_field_players": total_field_players,
            "total_goalkeepers": total_goalkeepers,
            "total_players": total_players,
            "teams_with_errors": teams_with_errors,
            "successful_teams_list": successful_teams,
        }

        # Print detailed report
        logging.info("\n" + "🏆 RUSSIAN PREMIER LEAGUE PARSING SUMMARY")
        logging.info("="*80)
        logging.info(f"\n📊 OVERALL STATISTICS:")
        logging.info(f"   Teams processed: {total_teams}")
        logging.info(f"   ✅ Successful: {len(successful_teams)}")
        logging.info(f"   ❌ Failed: {len(teams_with_errors)}")
        logging.info(f"\n👥 PLAYER STATISTICS:")
        logging.info(f"   ⚽ Field players: {total_field_players}")
        logging.info(f"   🥅 Goalkeepers: {total_goalkeepers}")
        logging.info(f"   📈 Total players: {total_players}")

        if successful_teams:
            logging.info(f"\n✅ SUCCESSFUL TEAMS ({len(successful_teams)}):")
            for team in sorted(successful_teams):
                team_result = next(r for r in results_list if r['team'] == team)
                logging.info(
                    f"   {team}: "
                    f"{team_result['field_players_count']} field + "
                    f"{team_result['goalkeepers_count']} GK = "
                    f"{team_result['total_players']} total"
                )

        if teams_with_errors:
            logging.warning(f"\n⚠️ TEAMS WITH ERRORS ({len(teams_with_errors)}):")
            for team in teams_with_errors:
                team_result = next(r for r in results_list if r['team'] == team)
                logging.warning(f"   {team}: {team_result.get('error', 'Unknown error')}")

        # Calculate average team size
        if successful_teams:
            avg_team_size = total_players / len(successful_teams)
            logging.info(f"\n📊 STATISTICS:")
            logging.info(f"   Average squad size: {avg_team_size:.1f} players")

        logging.info("\n" + "="*80)
        logging.info("🎉 RUSSIAN PREMIER LEAGUE PARSING COMPLETED!")
        logging.info("="*80)
        logging.info(f"📁 Output directory: /opt/airflow/data/russian_premier_league/")

        return summary

    # Define task dependencies
    squads = extract_premier_league_squads()
    parsing_results = parse_squad_players.expand(squad_info=squads)
    final_report = report_results(parsing_results)

    # Return final report for XCom visibility
    return final_report


# Instantiate the DAG
dag_instance = fbref_premier_league_parser()


# Documentation for Airflow UI
"""
## FBref Russian Premier League Parser DAG

### Overview
This DAG parses all players from all 16 Russian Premier League teams for the 2024-2025 season.

### Prerequisites
1. **FBref Pool**: Create pool in Airflow UI
   - Navigate to: Admin → Pools → Add
   - Pool name: `fbref_pool`
   - Slots: `1`
   - Description: "Rate limiting for FBref scraping"

2. **Dependencies**: Ensure fbref_parser package is installed
   - `fbref_parser` (field player and goalkeeper parsers)
   - `beautifulsoup4`, `cloudscraper`, `pandas`

### Rate Limiting
- **Pool**: Single-slot 'fbref_pool' ensures sequential execution
- **Delays**: 6-second delays between individual player requests
- **Limit**: Complies with FBref's 10 requests/minute restriction

### Expected Runtime
- **First run**: ~50 minutes (parsing ~400 players)
- **Subsequent runs**: Much faster (skips existing files)

### Output Structure
```
/opt/airflow/data/russian_premier_league/
├── zenit/
│   ├── player_name.csv
│   └── ...
├── spartak_moscow/
│   ├── player_name.csv
│   └── ...
└── ...
```

### Monitoring
- Check task logs for real-time progress
- Each team task shows: field players, goalkeepers, total
- Final report aggregates all statistics

### Error Handling
- Retries: 2 attempts per task with 5-minute delays
- Timeout: 1 hour per team, 2 hours overall
- Failed teams reported in final summary
- Existing files are skipped (idempotent)

### Manual Testing
Test individual components:
```bash
cd /root/data_platform/airflow/dags
python3 fbref_premier_league_utils.py  # Test squad extraction
```
"""
