"""airflow DAG: trigger-only daily ESPN Native Bronze v2 ingestion."""

from utils.espn_dag_factory import build_espn_ingest_dag


DAG_MODE = "daily"
dag = build_espn_ingest_dag(dag_id="dag_ingest_espn", mode=DAG_MODE)
