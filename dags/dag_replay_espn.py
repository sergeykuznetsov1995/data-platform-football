"""airflow DAG: manual zero-network ESPN replay from exact Raw manifests."""

from utils.espn_dag_factory import build_espn_ingest_dag


DAG_MODE = "replay"
dag = build_espn_ingest_dag(dag_id="dag_replay_espn", mode=DAG_MODE)
