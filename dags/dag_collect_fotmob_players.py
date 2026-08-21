"""Manual owner for missing FotMob player cards.

The collector is separate from catalog waves and history campaigns. It only
asks the ingest child for current-squad identities absent from the typed player
snapshot view. The DAG is deliberately trigger-only and starts paused so an
operator can run it inside a known free publication-writer window.
"""

from datetime import datetime, timedelta, timezone
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from scrapers.fotmob.player_collector import (
    PLAYER_COLLECTOR_MAX_DIRECT_MIB,
    PLAYER_COLLECTOR_MAX_REQUESTS,
    PLAYER_COLLECTOR_MODE,
    PLAYER_COLLECTOR_PLAYER_LIMIT,
    PLAYER_COLLECTOR_REQUESTS_PER_MINUTE,
)
from utils.default_args import DEFAULT_ARGS
from utils.fotmob_publication import (
    attest_fotmob_player_collector_runtime,
    fail_unsealed_fotmob_player_collector_publication,
    initialize_fotmob_player_collector_publication,
)


ISOLATED_STACK_ENV = "FOTMOB_ISOLATED_STACK"
INGEST_DAG_ID = "dag_ingest_fotmob"
INITIALIZER_TASK_ID = "initialize_fotmob_publication"
TRIGGER_TASK_ID = "trigger_fotmob_player_collector"
GENERATION_TEMPLATE = (
    "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
    "['generation_id'] }}"
)
BINDING_TEMPLATE = {
    key: (
        "{{ ti.xcom_pull(task_ids='initialize_fotmob_publication')"
        f"['binding']['{key}'] }}}}"
    )
    for key in (
        "schema",
        "source",
        "owner",
        "data_interval_start",
        "data_interval_end",
        "runtime_fingerprint",
    )
}


dag = None
if os.environ.get(ISOLATED_STACK_ENV) == "1":
    with DAG(
        dag_id="dag_collect_fotmob_players",
        description="Collect only missing current-squad FotMob player cards",
        default_args={**DEFAULT_ARGS, "retries": 0},
        schedule=None,
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=True,
        tags=["fotmob", "players", "collector", "bronze"],
    ) as dag:
        attest_runtime = PythonOperator(
            task_id="attest_isolated_runtime",
            python_callable=attest_fotmob_player_collector_runtime,
            retries=0,
        )

        initialize_publication = PythonOperator(
            task_id=INITIALIZER_TASK_ID,
            python_callable=initialize_fotmob_player_collector_publication,
            retries=0,
        )

        trigger_collector = TriggerDagRunOperator(
            task_id=TRIGGER_TASK_ID,
            trigger_dag_id=INGEST_DAG_ID,
            trigger_run_id="fotmob_players__" + GENERATION_TEMPLATE,
            logical_date="{{ logical_date.isoformat() }}",
            wait_for_completion=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
            reset_dag_run=False,
            conf={
                "mode": PLAYER_COLLECTOR_MODE,
                "scope": "",
                "catalog_contract": "",
                "daily_contract": "",
                "competition_scope_file": "",
                "competition_scope_sha256": "",
                "competition_ids_sha256": "",
                "source_refresh_profile": "",
                "source_refresh_targets_sha256": "",
                "entities": "players",
                "max_requests": PLAYER_COLLECTOR_MAX_REQUESTS,
                "max_direct_mib": PLAYER_COLLECTOR_MAX_DIRECT_MIB,
                "max_proxy_mib": 0,
                "competition_limit": 0,
                "season_limit": 0,
                "match_limit": 0,
                "team_limit": 0,
                "player_limit": PLAYER_COLLECTOR_PLAYER_LIMIT,
                "max_attempts": 4,
                "requests_per_minute": PLAYER_COLLECTOR_REQUESTS_PER_MINUTE,
                "deadline": "",
                "fotmob_publication": {
                    "generation_id": GENERATION_TEMPLATE,
                    "binding": BINDING_TEMPLATE,
                },
            },
            execution_timeout=timedelta(hours=1),
            retries=0,
        )

        finalize_publication = PythonOperator(
            task_id="finalize_fotmob_publication",
            python_callable=fail_unsealed_fotmob_player_collector_publication,
            op_kwargs={
                "success_task_id": TRIGGER_TASK_ID,
                "writer_task_ids": [TRIGGER_TASK_ID],
            },
            trigger_rule="all_done",
            retries=0,
        )

        (
            attest_runtime
            >> initialize_publication
            >> trigger_collector
            >> finalize_publication
        )


__all__ = ["dag"]
