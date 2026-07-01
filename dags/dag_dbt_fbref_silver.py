"""
dbt-trino Silver (FBref) — Cosmos-rendered DAG  (Tier-2 pilot)
==============================================================

Renders the ``football_transform`` dbt project's ``silver`` FBref models as an
Airflow TaskGroup via astronomer-cosmos. This is the pilot replacement for the
inline-CTAS ``dag_transform_fbref_silver`` — the legacy DAG is intentionally
left in place for A/B parity until cutover.

Pipeline shape (Cosmos expands one Airflow task per dbt node):
    source(bronze.*) -> model(silver.fbref_*) -> dbt tests (AFTER_EACH)

Config:
    * Project  : /opt/airflow/transform   (mounted from ./transform)
    * Profile  : transform/profiles.yml    (Trino over HTTPS, env-driven)
    * Models   : models/silver/fbref_*.sql (+ dbt tests in _silver__models.yml)
    * Execution: LOCAL — dbt-trino lives in an isolated venv
      (/home/airflow/dbt-venv) to avoid dbt-core<->Airflow dep conflicts;
      Cosmos invokes it as a subprocess.
    * Render   : DBT_LS (Cosmos runs `dbt ls`; no warehouse connection at parse).

Trigger-only (no schedule), mirroring ``dag_transform_fbref_silver``.

Prereqs (deploy steps, not repo):
    * Airflow image rebuilt with dbt-trino + astronomer-cosmos (requirements.txt).
    * Trino self-signed cert available at ``TRINO_CERT_PATH`` (see profiles.yml).
    * Bronze FBref tables populated (same precondition as the legacy DAG).
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from cosmos import (
    DbtDag,
    ExecutionConfig,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)
from cosmos.constants import LoadMode, TestBehavior

DBT_PROJECT_DIR = Path("/opt/airflow/transform")

profile_config = ProfileConfig(
    profile_name="football",
    target_name="dev",
    # Reuse the project's profiles.yml (env-driven: TRINO_USER/PASSWORD/HOST/
    # PORT/CERT are already present in the Airflow container environment).
    profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
)

dag = DbtDag(
    dag_id="dag_dbt_fbref_silver",
    project_config=ProjectConfig(DBT_PROJECT_DIR),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/home/airflow/dbt-venv/bin/dbt",
    ),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        # Only the FBref Silver pilot scope.
        select=["path:models/silver"],
        # Run each model's dbt tests immediately after it builds (mirrors the
        # legacy DAG's per-transform DQ gate).
        test_behavior=TestBehavior.AFTER_EACH,
        # Install dbt_utils (used by the uniqueness tests) before rendering/run.
        dbt_deps=True,
    ),
    # Trigger-only, matching dag_transform_fbref_silver.
    schedule=None,
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=["transform", "fbref", "silver", "dbt", "trino"],
    default_args={"retries": 1},
    doc_md=__doc__,
)
