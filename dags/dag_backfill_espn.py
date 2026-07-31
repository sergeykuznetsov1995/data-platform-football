"""Manual ESPN historical backfill DAG with explicit promoted scopes."""

from utils.espn_dag_factory import build_espn_ingest_dag


DAG_MODE = "backfill"
dag = build_espn_ingest_dag(dag_id="dag_backfill_espn", mode=DAG_MODE)
