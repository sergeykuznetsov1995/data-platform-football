"""
Airflow DAG for uploading parsed football data to HDFS in Parquet format

This DAG processes CSV files from the Premier League parser and:
1. Converts CSV files to Parquet format for better performance
2. Uploads Parquet files to HDFS
3. Creates/updates Hive tables for Trino access

Schedule: Manual trigger (or after fbref_premier_league_parser completion)
Expected runtime: ~5-10 minutes

Architecture:
    1. scan_local_data: Find all CSV files in data directory
    2. convert_to_parquet: Convert CSV to Parquet format
    3. upload_to_hdfs: Upload Parquet files to HDFS
    4. report_upload_results: Generate summary report

HDFS Structure:
    /data/premier_league/
        field_players/
            team=arsenal/
                data.parquet
            team=manchester_city/
                data.parquet
        goalkeepers/
            team=arsenal/
                data.parquet
            ...

Trino Access:
    SELECT * FROM hive.premier_league.field_players WHERE team = 'arsenal'
"""

from airflow.decorators import dag, task
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
import os


# Constants
LOCAL_DATA_DIR = "/opt/airflow/data/leagues"
HDFS_BASE_PATH = "/data/all_leagues"
WEBHDFS_URL = "http://namenode:9870/webhdfs/v1"
PARQUET_OUTPUT_DIR = "/opt/airflow/data/parquet"
TRINO_HOST = "trino-coordinator"
TRINO_PORT = 8085
TRINO_CATALOG = "hive"
TRINO_SCHEMA = "all_leagues"


def apply_schema_to_dataframe(df):
    """
    Apply consistent schema to DataFrame - convert all int columns to DOUBLE.

    This ensures Parquet schema matches Trino DDL expectations.

    Problem:
    - Pandas auto-detects int64 for columns like 'starts_total', 'mp', 'age'
    - DDL defines these as DOUBLE
    - Different teams may have NaN values causing schema inconsistency
    - Trino can't read mixed int64/double across partitions
    - CRITICAL: Some CSV files have numeric season values (2019, 2020) that pandas reads as int64

    Solution:
    - Explicitly convert ALL int64 columns to float64 (DOUBLE in Trino)
    - Force convert season to string (handles cases where pandas reads it as int64)
    - This allows NaN values and ensures schema consistency

    Data verified:
    - Field players: minutes, matches_completed contain NaN
    - Goalkeepers: 215 of 260 numeric columns contain NaN
    - Season can be int64 in CSV files with numeric values (e.g., 2019 instead of 2019-2020)

    Args:
        df: pandas DataFrame to transform

    Returns:
        DataFrame with all integer columns converted to float64 and season as string
    """
    import pandas as pd

    # CRITICAL FIX: Force season to string (handles numeric season values like 2019)
    if 'season' in df.columns:
        df['season'] = df['season'].astype(str)

    # Convert ALL integer columns to float64 to handle NaN and match DDL
    for col in df.columns:
        # Skip string columns
        if col in ['season', 'squad', 'country', 'competition', 'player_name', 'team', 'lgrank']:
            continue

        # Convert any integer type to float64
        if df[col].dtype in ['int64', 'int32', 'int16', 'int8']:
            df[col] = df[col].astype('float64')

    return df


default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(hours=1),
}


@dag(
    dag_id='hdfs_data_pipeline',
    default_args=default_args,
    description='Upload Premier League data to HDFS in Parquet format',
    schedule=None,
    start_date=datetime(2024, 11, 20),
    catchup=False,
    max_active_runs=1,
    tags=['hdfs', 'parquet', 'premier_league', 'data_pipeline'],
    doc_md=__doc__,
)
def hdfs_data_pipeline():
    """
    Main DAG for uploading Premier League data to HDFS
    """

    @task(task_id='scan_local_data')
    def scan_local_data() -> Dict[str, List[str]]:
        """
        Scan local data directory for CSV files from all leagues

        Returns:
            Dict with 'field_players', 'goalkeepers', and 'leagues' lists
        """
        logging.info("=" * 80)
        logging.info("TASK: Scan Local Data (All Leagues)")
        logging.info("=" * 80)

        result = {
            'field_players': [],
            'goalkeepers': [],
            'leagues': []
        }

        if not os.path.exists(LOCAL_DATA_DIR):
            logging.warning(f"Data directory not found: {LOCAL_DATA_DIR}")
            return result

        # Iterate through leagues
        leagues = [d for d in os.listdir(LOCAL_DATA_DIR)
                   if os.path.isdir(os.path.join(LOCAL_DATA_DIR, d))]

        for league_name in leagues:
            league_path = os.path.join(LOCAL_DATA_DIR, league_name)
            result['leagues'].append(league_name)

            # Iterate through teams in each league
            teams = [d for d in os.listdir(league_path)
                     if os.path.isdir(os.path.join(league_path, d))]

            for team_name in teams:
                team_path = os.path.join(league_path, team_name)

                # Field players
                fp_dir = os.path.join(team_path, 'field_players')
                if os.path.exists(fp_dir):
                    for f in os.listdir(fp_dir):
                        if f.endswith('.csv'):
                            result['field_players'].append({
                                'league': league_name,
                                'team': team_name,
                                'file': os.path.join(fp_dir, f),
                                'player': f.replace('.csv', '')
                            })

                # Goalkeepers
                gk_dir = os.path.join(team_path, 'goalkeepers')
                if os.path.exists(gk_dir):
                    for f in os.listdir(gk_dir):
                        if f.endswith('.csv'):
                            result['goalkeepers'].append({
                                'league': league_name,
                                'team': team_name,
                                'file': os.path.join(gk_dir, f),
                                'player': f.replace('.csv', '')
                            })

        logging.info(f"Found {len(result['leagues'])} leagues")
        logging.info(f"Found {len(result['field_players'])} field player files")
        logging.info(f"Found {len(result['goalkeepers'])} goalkeeper files")

        return result

    @task(task_id='convert_to_parquet')
    def convert_to_parquet(scan_result: Dict[str, List]) -> Dict[str, Any]:
        """
        Convert CSV files to Parquet format grouped by team

        Args:
            scan_result: Dict with file lists from scan_local_data

        Returns:
            Dict with paths to created Parquet files
        """
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        logging.info("=" * 80)
        logging.info("TASK: Convert to Parquet")
        logging.info("=" * 80)

        os.makedirs(PARQUET_OUTPUT_DIR, exist_ok=True)
        os.makedirs(os.path.join(PARQUET_OUTPUT_DIR, 'field_players'), exist_ok=True)
        os.makedirs(os.path.join(PARQUET_OUTPUT_DIR, 'goalkeepers'), exist_ok=True)

        result = {
            'field_players_parquet': [],
            'goalkeepers_parquet': [],
            'errors': []
        }

        # Group files by (league, team)
        teams_fp = {}
        for item in scan_result.get('field_players', []):
            league = item['league']
            team = item['team']
            key = (league, team)
            if key not in teams_fp:
                teams_fp[key] = []
            teams_fp[key].append(item)

        teams_gk = {}
        for item in scan_result.get('goalkeepers', []):
            league = item['league']
            team = item['team']
            key = (league, team)
            if key not in teams_gk:
                teams_gk[key] = []
            teams_gk[key].append(item)

        # Convert field players by (league, team)
        for (league, team), files in teams_fp.items():
            try:
                dfs = []
                for item in files:
                    df = pd.read_csv(item['file'])
                    df['player_name'] = item['player']
                    df['team'] = team
                    df['league'] = league
                    dfs.append(df)

                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)

                    # Apply schema transformations to ensure all int64 columns become float64 (DOUBLE)
                    # This matches Trino DDL and handles NaN values correctly
                    combined_df = apply_schema_to_dataframe(combined_df)

                    output_path = os.path.join(
                        PARQUET_OUTPUT_DIR, 'field_players',
                        f'league={league}', f'team={team}', 'data.parquet'
                    )
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    combined_df.to_parquet(output_path, index=False, compression='snappy')
                    result['field_players_parquet'].append({
                        'league': league,
                        'team': team,
                        'path': output_path,
                        'rows': len(combined_df),
                        'players': len(files)
                    })
                    logging.info(f"Created Parquet for {league}/{team} field players: {len(combined_df)} rows")
            except Exception as e:
                logging.error(f"Error converting {league}/{team} field players: {e}")
                result['errors'].append(f"field_players/{league}/{team}: {str(e)}")

        # Convert goalkeepers by (league, team)
        for (league, team), files in teams_gk.items():
            try:
                dfs = []
                for item in files:
                    df = pd.read_csv(item['file'])
                    df['player_name'] = item['player']
                    df['team'] = team
                    df['league'] = league
                    dfs.append(df)

                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)

                    # Apply schema transformations to ensure all int64 columns become float64 (DOUBLE)
                    # This matches Trino DDL and handles NaN values correctly
                    combined_df = apply_schema_to_dataframe(combined_df)

                    output_path = os.path.join(
                        PARQUET_OUTPUT_DIR, 'goalkeepers',
                        f'league={league}', f'team={team}', 'data.parquet'
                    )
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    combined_df.to_parquet(output_path, index=False, compression='snappy')
                    result['goalkeepers_parquet'].append({
                        'league': league,
                        'team': team,
                        'path': output_path,
                        'rows': len(combined_df),
                        'players': len(files)
                    })
                    logging.info(f"Created Parquet for {league}/{team} goalkeepers: {len(combined_df)} rows")
            except Exception as e:
                logging.error(f"Error converting {league}/{team} goalkeepers: {e}")
                result['errors'].append(f"goalkeepers/{league}/{team}: {str(e)}")

        logging.info(f"Created {len(result['field_players_parquet'])} field player Parquet files")
        logging.info(f"Created {len(result['goalkeepers_parquet'])} goalkeeper Parquet files")

        return result

    @task(task_id='upload_to_hdfs')
    def upload_to_hdfs(parquet_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Upload Parquet files to HDFS using WebHDFS

        Args:
            parquet_result: Dict with Parquet file paths

        Returns:
            Dict with upload statistics
        """
        import requests

        logging.info("=" * 80)
        logging.info("TASK: Upload to HDFS")
        logging.info("=" * 80)

        result = {
            'uploaded_field_players': 0,
            'uploaded_goalkeepers': 0,
            'errors': [],
            'hdfs_paths': []
        }

        def upload_file(local_path: str, hdfs_path: str) -> bool:
            """Upload single file to HDFS via WebHDFS"""
            try:
                # Create directory
                dir_path = os.path.dirname(hdfs_path)
                create_dir_url = f"{WEBHDFS_URL}{dir_path}?op=MKDIRS&user.name=root"
                requests.put(create_dir_url, timeout=30)

                # Upload file
                create_url = f"{WEBHDFS_URL}{hdfs_path}?op=CREATE&user.name=root&overwrite=true"
                response = requests.put(create_url, allow_redirects=False, timeout=30)

                if response.status_code == 307:
                    redirect_url = response.headers['Location']
                    with open(local_path, 'rb') as f:
                        upload_response = requests.put(
                            redirect_url,
                            data=f,
                            headers={'Content-Type': 'application/octet-stream'},
                            timeout=120
                        )
                        return upload_response.status_code == 201
                return False
            except Exception as e:
                logging.error(f"Upload error for {hdfs_path}: {e}")
                return False

        # Upload field players
        for item in parquet_result.get('field_players_parquet', []):
            league = item['league']
            team = item['team']
            local_path = item['path']
            hdfs_path = f"{HDFS_BASE_PATH}/field_players/league={league}/team={team}/data.parquet"

            if upload_file(local_path, hdfs_path):
                result['uploaded_field_players'] += 1
                result['hdfs_paths'].append(hdfs_path)
                logging.info(f"Uploaded: {hdfs_path}")
            else:
                result['errors'].append(f"Failed to upload: {hdfs_path}")

        # Upload goalkeepers
        for item in parquet_result.get('goalkeepers_parquet', []):
            league = item['league']
            team = item['team']
            local_path = item['path']
            hdfs_path = f"{HDFS_BASE_PATH}/goalkeepers/league={league}/team={team}/data.parquet"

            if upload_file(local_path, hdfs_path):
                result['uploaded_goalkeepers'] += 1
                result['hdfs_paths'].append(hdfs_path)
                logging.info(f"Uploaded: {hdfs_path}")
            else:
                result['errors'].append(f"Failed to upload: {hdfs_path}")

        logging.info(f"Uploaded {result['uploaded_field_players']} field player files")
        logging.info(f"Uploaded {result['uploaded_goalkeepers']} goalkeeper files")

        return result

    @task(task_id='sync_partitions')
    def sync_partitions(upload_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Synchronize partition metadata in Trino/Hive after HDFS upload

        This ensures Trino can see all partitions (leagues/teams) that were uploaded to HDFS.
        Without this step, Trino only sees partitions that existed when tables were created.

        Args:
            upload_result: Dict with upload statistics

        Returns:
            Dict with sync results
        """
        import trino

        logging.info("=" * 80)
        logging.info("TASK: Sync Partition Metadata")
        logging.info("=" * 80)

        result = {
            'field_players_synced': False,
            'goalkeepers_synced': False,
            'errors': []
        }

        try:
            # Connect to Trino coordinator
            conn = trino.dbapi.connect(
                host=TRINO_HOST,
                port=TRINO_PORT,
                user='airflow',
                catalog=TRINO_CATALOG,
                schema=TRINO_SCHEMA,
            )
            cursor = conn.cursor()

            # Sync field_players table partitions
            try:
                logging.info("Syncing field_players table partitions...")
                cursor.execute("CALL system.sync_partition_metadata('all_leagues', 'field_players', 'FULL')")
                result['field_players_synced'] = True
                logging.info("✅ field_players partitions synced successfully")
            except Exception as e:
                error_msg = f"Failed to sync field_players: {str(e)}"
                logging.error(error_msg)
                result['errors'].append(error_msg)

            # Sync goalkeepers table partitions
            try:
                logging.info("Syncing goalkeepers table partitions...")
                cursor.execute("CALL system.sync_partition_metadata('all_leagues', 'goalkeepers', 'FULL')")
                result['goalkeepers_synced'] = True
                logging.info("✅ goalkeepers partitions synced successfully")
            except Exception as e:
                error_msg = f"Failed to sync goalkeepers: {str(e)}"
                logging.error(error_msg)
                result['errors'].append(error_msg)

            cursor.close()
            conn.close()

            logging.info(f"\n📊 SYNC SUMMARY:")
            logging.info(f"   Field players synced: {result['field_players_synced']}")
            logging.info(f"   Goalkeepers synced: {result['goalkeepers_synced']}")

            if result['errors']:
                logging.warning(f"\n⚠️ ERRORS ({len(result['errors'])}):")
                for error in result['errors']:
                    logging.warning(f"   - {error}")

        except Exception as e:
            error_msg = f"Failed to connect to Trino: {str(e)}"
            logging.error(error_msg)
            result['errors'].append(error_msg)
            raise

        return result

    @task(task_id='report_upload_results')
    def report_upload_results(
        scan_result: Dict,
        parquet_result: Dict,
        upload_result: Dict,
        sync_result: Dict | None = None
    ) -> Dict[str, Any]:
        """
        Generate final report of the upload process
        """
        logging.info("=" * 80)
        logging.info("HDFS DATA PIPELINE - FINAL REPORT (ALL LEAGUES)")
        logging.info("=" * 80)

        summary = {
            'leagues_processed': len(scan_result.get('leagues', [])),
            'csv_field_players': len(scan_result.get('field_players', [])),
            'csv_goalkeepers': len(scan_result.get('goalkeepers', [])),
            'parquet_field_players': len(parquet_result.get('field_players_parquet', [])),
            'parquet_goalkeepers': len(parquet_result.get('goalkeepers_parquet', [])),
            'hdfs_uploaded_fp': upload_result.get('uploaded_field_players', 0),
            'hdfs_uploaded_gk': upload_result.get('uploaded_goalkeepers', 0),
            'sync_field_players': bool(sync_result and sync_result.get('field_players_synced')),
            'sync_goalkeepers': bool(sync_result and sync_result.get('goalkeepers_synced')),
            'errors': parquet_result.get('errors', []) + upload_result.get('errors', [])
        }

        if sync_result:
            summary['errors'].extend(sync_result.get('errors', []))

        logging.info(f"\n📊 SUMMARY:")
        logging.info(f"   Leagues processed: {summary['leagues_processed']}")
        logging.info(f"\n📁 CSV FILES:")
        logging.info(f"   Field players: {summary['csv_field_players']}")
        logging.info(f"   Goalkeepers: {summary['csv_goalkeepers']}")
        logging.info(f"\n📦 PARQUET FILES:")
        logging.info(f"   Field players: {summary['parquet_field_players']}")
        logging.info(f"   Goalkeepers: {summary['parquet_goalkeepers']}")
        logging.info(f"\n☁️ HDFS UPLOADS:")
        logging.info(f"   Field players: {summary['hdfs_uploaded_fp']}")
        logging.info(f"   Goalkeepers: {summary['hdfs_uploaded_gk']}")
        logging.info(f"\n🗃️ PARTITION SYNC:")
        logging.info(f"   Field players: {summary['sync_field_players']}")
        logging.info(f"   Goalkeepers: {summary['sync_goalkeepers']}")

        if summary['errors']:
            logging.warning(f"\n⚠️ ERRORS ({len(summary['errors'])}):")
            for error in summary['errors']:
                logging.warning(f"   - {error}")

        logging.info(f"\n🔗 HDFS PATH: hdfs://namenode:8020{HDFS_BASE_PATH}")
        logging.info(f"🌐 TRINO ACCESS:")
        logging.info(f"   SELECT * FROM hive.all_leagues.field_players LIMIT 10;")
        logging.info(f"   SELECT * FROM hive.all_leagues.goalkeepers LIMIT 10;")
        logging.info(f"\n📈 EXAMPLE QUERIES:")
        logging.info(f"   -- Filter by league:")
        logging.info(f"   SELECT * FROM hive.all_leagues.field_players WHERE league = 'a_league_men';")
        logging.info(f"   -- Top scorers across all leagues:")
        logging.info(f"   SELECT league, player_name, performance_gls FROM hive.all_leagues.field_players ORDER BY performance_gls DESC LIMIT 20;")

        logging.info("\n" + "=" * 80)
        logging.info("HDFS DATA PIPELINE COMPLETED!")
        logging.info("=" * 80)

        return summary

    # Task dependencies
    scan = scan_local_data()
    parquet = convert_to_parquet(scan)
    upload = upload_to_hdfs(parquet)
    sync = sync_partitions(upload)
    report = report_upload_results(scan, parquet, upload, sync)

    return report


dag_instance = hdfs_data_pipeline()
