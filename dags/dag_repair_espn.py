"""Manual ESPN repair DAG with explicit promoted scopes."""

from utils.espn_dag_factory import build_espn_ingest_dag


DAG_MODE = "repair"
dag = build_espn_ingest_dag(dag_id="dag_repair_espn", mode=DAG_MODE)
