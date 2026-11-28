# ---- hunk 1: -36,7 +36,8 ----

METADB = "postgres"          # общий стек (morning_report.METADBS)
DAG_ID = "dag_ingest_clubelo"
TRINO = "/root/.claude/bin/trino-ro.sh"
SNAPSHOT = "iceberg.bronze.clubelo_rank_snapshot"
RAW = "iceberg.bronze.clubelo_raw_page"
STATE_DEFAULT = Path("/root/watchdog/state/clubelo_stall_state.json")
