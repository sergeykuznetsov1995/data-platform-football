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
LOCAL_DATA_DIR = "/opt/airflow/data/premier_league"
HDFS_BASE_PATH = "/data/premier_league"
WEBHDFS_URL = "http://namenode:9870/webhdfs/v1"
PARQUET_OUTPUT_DIR = "/opt/airflow/data/parquet"


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
        Scan local data directory for CSV files

        Returns:
            Dict with 'field_players' and 'goalkeepers' lists of file paths
        """
        logging.info("=" * 80)
        logging.info("TASK: Scan Local Data")
        logging.info("=" * 80)

        result = {
            'field_players': [],
            'goalkeepers': [],
            'teams': []
        }

        if not os.path.exists(LOCAL_DATA_DIR):
            logging.warning(f"Data directory not found: {LOCAL_DATA_DIR}")
            return result

        teams = [d for d in os.listdir(LOCAL_DATA_DIR)
                 if os.path.isdir(os.path.join(LOCAL_DATA_DIR, d))]

        for team in teams:
            team_dir = os.path.join(LOCAL_DATA_DIR, team)
            result['teams'].append(team)

            # Field players
            fp_dir = os.path.join(team_dir, 'field_players')
            if os.path.exists(fp_dir):
                for f in os.listdir(fp_dir):
                    if f.endswith('.csv'):
                        result['field_players'].append({
                            'team': team,
                            'file': os.path.join(fp_dir, f),
                            'player': f.replace('.csv', '')
                        })

            # Goalkeepers
            gk_dir = os.path.join(team_dir, 'goalkeepers')
            if os.path.exists(gk_dir):
                for f in os.listdir(gk_dir):
                    if f.endswith('.csv'):
                        result['goalkeepers'].append({
                            'team': team,
                            'file': os.path.join(gk_dir, f),
                            'player': f.replace('.csv', '')
                        })

        logging.info(f"Found {len(result['teams'])} teams")
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

        # Group files by team
        teams_fp = {}
        for item in scan_result.get('field_players', []):
            team = item['team']
            if team not in teams_fp:
                teams_fp[team] = []
            teams_fp[team].append(item)

        teams_gk = {}
        for item in scan_result.get('goalkeepers', []):
            team = item['team']
            if team not in teams_gk:
                teams_gk[team] = []
            teams_gk[team].append(item)

        # Convert field players by team
        for team, files in teams_fp.items():
            try:
                dfs = []
                for item in files:
                    df = pd.read_csv(item['file'])
                    df['player_name'] = item['player']
                    df['team'] = team
                    dfs.append(df)

                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)
                    output_path = os.path.join(
                        PARQUET_OUTPUT_DIR, 'field_players', f'team={team}', 'data.parquet'
                    )
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    combined_df.to_parquet(output_path, index=False, compression='snappy')
                    result['field_players_parquet'].append({
                        'team': team,
                        'path': output_path,
                        'rows': len(combined_df),
                        'players': len(files)
                    })
                    logging.info(f"Created Parquet for {team} field players: {len(combined_df)} rows")
            except Exception as e:
                logging.error(f"Error converting {team} field players: {e}")
                result['errors'].append(f"field_players/{team}: {str(e)}")

        # Convert goalkeepers by team
        for team, files in teams_gk.items():
            try:
                dfs = []
                for item in files:
                    df = pd.read_csv(item['file'])
                    df['player_name'] = item['player']
                    df['team'] = team
                    dfs.append(df)

                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)
                    output_path = os.path.join(
                        PARQUET_OUTPUT_DIR, 'goalkeepers', f'team={team}', 'data.parquet'
                    )
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    combined_df.to_parquet(output_path, index=False, compression='snappy')
                    result['goalkeepers_parquet'].append({
                        'team': team,
                        'path': output_path,
                        'rows': len(combined_df),
                        'players': len(files)
                    })
                    logging.info(f"Created Parquet for {team} goalkeepers: {len(combined_df)} rows")
            except Exception as e:
                logging.error(f"Error converting {team} goalkeepers: {e}")
                result['errors'].append(f"goalkeepers/{team}: {str(e)}")

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
            team = item['team']
            local_path = item['path']
            hdfs_path = f"{HDFS_BASE_PATH}/field_players/team={team}/data.parquet"

            if upload_file(local_path, hdfs_path):
                result['uploaded_field_players'] += 1
                result['hdfs_paths'].append(hdfs_path)
                logging.info(f"Uploaded: {hdfs_path}")
            else:
                result['errors'].append(f"Failed to upload: {hdfs_path}")

        # Upload goalkeepers
        for item in parquet_result.get('goalkeepers_parquet', []):
            team = item['team']
            local_path = item['path']
            hdfs_path = f"{HDFS_BASE_PATH}/goalkeepers/team={team}/data.parquet"

            if upload_file(local_path, hdfs_path):
                result['uploaded_goalkeepers'] += 1
                result['hdfs_paths'].append(hdfs_path)
                logging.info(f"Uploaded: {hdfs_path}")
            else:
                result['errors'].append(f"Failed to upload: {hdfs_path}")

        logging.info(f"Uploaded {result['uploaded_field_players']} field player files")
        logging.info(f"Uploaded {result['uploaded_goalkeepers']} goalkeeper files")

        return result

    @task(task_id='report_upload_results')
    def report_upload_results(
        scan_result: Dict,
        parquet_result: Dict,
        upload_result: Dict
    ) -> Dict[str, Any]:
        """
        Generate final report of the upload process
        """
        logging.info("=" * 80)
        logging.info("HDFS DATA PIPELINE - FINAL REPORT")
        logging.info("=" * 80)

        summary = {
            'teams_processed': len(scan_result.get('teams', [])),
            'csv_field_players': len(scan_result.get('field_players', [])),
            'csv_goalkeepers': len(scan_result.get('goalkeepers', [])),
            'parquet_field_players': len(parquet_result.get('field_players_parquet', [])),
            'parquet_goalkeepers': len(parquet_result.get('goalkeepers_parquet', [])),
            'hdfs_uploaded_fp': upload_result.get('uploaded_field_players', 0),
            'hdfs_uploaded_gk': upload_result.get('uploaded_goalkeepers', 0),
            'errors': parquet_result.get('errors', []) + upload_result.get('errors', [])
        }

        logging.info(f"\n📊 SUMMARY:")
        logging.info(f"   Teams processed: {summary['teams_processed']}")
        logging.info(f"\n📁 CSV FILES:")
        logging.info(f"   Field players: {summary['csv_field_players']}")
        logging.info(f"   Goalkeepers: {summary['csv_goalkeepers']}")
        logging.info(f"\n📦 PARQUET FILES:")
        logging.info(f"   Field players: {summary['parquet_field_players']}")
        logging.info(f"   Goalkeepers: {summary['parquet_goalkeepers']}")
        logging.info(f"\n☁️ HDFS UPLOADS:")
        logging.info(f"   Field players: {summary['hdfs_uploaded_fp']}")
        logging.info(f"   Goalkeepers: {summary['hdfs_uploaded_gk']}")

        if summary['errors']:
            logging.warning(f"\n⚠️ ERRORS ({len(summary['errors'])}):")
            for error in summary['errors']:
                logging.warning(f"   - {error}")

        logging.info(f"\n🔗 HDFS PATH: hdfs://namenode:8020{HDFS_BASE_PATH}")
        logging.info(f"🌐 TRINO ACCESS:")
        logging.info(f"   SELECT * FROM hive.premier_league.field_players")
        logging.info(f"   SELECT * FROM hive.premier_league.goalkeepers")

        logging.info("\n" + "=" * 80)
        logging.info("HDFS DATA PIPELINE COMPLETED!")
        logging.info("=" * 80)

        return summary

    # Task dependencies
    scan = scan_local_data()
    parquet = convert_to_parquet(scan)
    upload = upload_to_hdfs(parquet)
    report = report_upload_results(scan, parquet, upload)

    return report


dag_instance = hdfs_data_pipeline()
