"""
Pytest fixtures for DAG integration tests.
"""

import os
import sys
from pathlib import Path

import pytest


# Add project root and dags folder to path for imports
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
DAGS_FOLDER = PROJECT_ROOT / 'dags'
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(DAGS_FOLDER))


@pytest.fixture(scope='session')
def dags_folder():
    """Return path to DAGs folder."""
    return PROJECT_ROOT / 'dags'


@pytest.fixture(scope='session')
def dag_bag(dags_folder):
    """
    Create a DagBag for testing DAGs.

    Returns:
        Airflow DagBag instance
    """
    # Set required Airflow environment variables
    os.environ.setdefault('AIRFLOW_HOME', str(PROJECT_ROOT / 'airflow_home'))
    os.environ.setdefault('AIRFLOW__CORE__DAGS_FOLDER', str(dags_folder))
    os.environ.setdefault('AIRFLOW__CORE__LOAD_EXAMPLES', 'False')
    os.environ.setdefault('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN', 'sqlite:///airflow.db')

    try:
        from airflow.models import DagBag
        return DagBag(dag_folder=str(dags_folder), include_examples=False)
    except ImportError:
        pytest.skip("Airflow not installed")


@pytest.fixture(scope='session')
def expected_dag_ids():
    """Return list of expected DAG IDs."""
    return [
        'dag_ingest_fbref',
        'dag_ingest_fotmob',
        'dag_ingest_matchhistory',
        'dag_ingest_understat',
        'dag_ingest_whoscored',
        'dag_ingest_sofascore',
        'dag_ingest_espn',
        'dag_ingest_clubelo',
        'dag_ingest_sofifa',
        'dag_master_pipeline',
    ]


@pytest.fixture(scope='session')
def ingestion_dag_ids():
    """Return list of ingestion DAG IDs (excluding master)."""
    return [
        'dag_ingest_fbref',
        'dag_ingest_fotmob',
        'dag_ingest_matchhistory',
        'dag_ingest_understat',
        'dag_ingest_whoscored',
        'dag_ingest_sofascore',
        'dag_ingest_espn',
        'dag_ingest_clubelo',
        'dag_ingest_sofifa',
    ]
