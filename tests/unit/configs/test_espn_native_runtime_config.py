"""Runtime ownership contracts for ESPN Native Bronze v2."""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import subprocess

from dags.utils.config import SCHEDULES


ROOT = Path(__file__).resolve().parents[3]
AIRFLOW_SERVICES = ("airflow-init", "airflow-scheduler", "airflow-webserver")
CORE_CATALOG_URL = (
    "https://sports.core.api.espn.com/v2/sports/soccer/leagues"
    "?limit=500&lang=en&region=us"
)


def _render_compose(*, frozen_ref: tuple[str, str] | None = None) -> dict:
    environment = os.environ.copy()
    environment.pop("COMPOSE_FILE", None)
    environment.pop("COMPOSE_PROFILES", None)
    environment.update(
        {
            "PUBLIC_IP": "127.0.0.1",
            "KC_PUBLIC_URL": "https://auth.example.test",
            "OIDC_ISSUER": "https://auth.example.test/realms/test",
            "JUPYTER_PUBLIC_HOST": "jupyter.example.test",
            "TRINO_PUBLIC_HOST": "trino.example.test",
            "JUPYTERHUB_OIDC_CLIENT_SECRET": "compose-test",
            "KC_BOOTSTRAP_ADMIN_PASSWORD": "compose-test",
            "KEYCLOAK_DB_PASSWORD": "compose-test",
            "LAKEKEEPER_DB_PASSWORD": "compose-test",
            "LAKEKEEPER_PG_ENCRYPTION_KEY": "compose-test",
            "TRINO_ANALYST_SVC_PASSWORD": "compose-test",
            "FBREF_CAMOUFOX_GEOIP_DATABASE_HOST_PATH": "/tmp/geo.mmdb",
        }
    )
    for name in (
        "ESPN_DISCOVERY_STATE_REF_URI",
        "ESPN_DISCOVERY_STATE_REF_SHA256",
    ):
        environment.pop(name, None)
    if frozen_ref is not None:
        environment.update(
            ESPN_DISCOVERY_STATE_REF_URI=frozen_ref[0],
            ESPN_DISCOVERY_STATE_REF_SHA256=frozen_ref[1],
        )
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            str(ROOT / ".env.example"),
            "-f",
            str(ROOT / "compose.yaml"),
            "config",
            "--format",
            "json",
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_master_is_the_only_daily_espn_schedule_owner() -> None:
    """The ESPN child is trigger-only; the master owns the daily cadence."""
    assert SCHEDULES["dag_ingest_espn"] is None
    assert SCHEDULES["dag_master_pipeline"] == "0 14 * * *"


def test_native_espn_factory_declares_a_nonempty_description() -> None:
    """All factory-built ESPN DAGs must stay useful in the Airflow UI."""
    source = (ROOT / "dags/utils/espn_dag_factory.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    dag_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "DAG"
    ]

    assert len(dag_calls) == 1
    description = next(
        (
            keyword.value
            for keyword in dag_calls[0].keywords
            if keyword.arg == "description"
        ),
        None,
    )
    assert description is not None
    assert not (isinstance(description, ast.Constant) and not description.value)


def test_rendered_airflow_compose_uses_authoritative_core_catalog() -> None:
    services = _render_compose()["services"]

    for name in AIRFLOW_SERVICES:
        environment = services[name]["environment"]
        assert environment["ESPN_DISCOVERY_CATALOG_URL"] == CORE_CATALOG_URL
        assert "site.api.espn.com/apis/site/v2/sports/soccer/leagues" not in str(
            environment
        )


def test_rendered_airflow_compose_omits_or_propagates_complete_freeze_pair() -> None:
    unfrozen = _render_compose()["services"]
    for name in AIRFLOW_SERVICES:
        environment = unfrozen[name]["environment"]
        assert environment.get("ESPN_DISCOVERY_STATE_REF_URI") is None
        assert environment.get("ESPN_DISCOVERY_STATE_REF_SHA256") is None
        assert environment.get("ESPN_DISCOVERY_STATE_REF_URI") != ""
        assert environment.get("ESPN_DISCOVERY_STATE_REF_SHA256") != ""

    frozen_ref = (
        "s3://football/artifacts/espn/discovery/run/discovery-state.json",
        "a" * 64,
    )
    frozen = _render_compose(frozen_ref=frozen_ref)["services"]
    for name in AIRFLOW_SERVICES:
        environment = frozen[name]["environment"]
        assert environment["ESPN_DISCOVERY_STATE_REF_URI"] == frozen_ref[0]
        assert environment["ESPN_DISCOVERY_STATE_REF_SHA256"] == frozen_ref[1]
