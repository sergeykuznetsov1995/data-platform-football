"""Runtime ownership contracts for ESPN Native Bronze v2."""

from __future__ import annotations

import ast
import json
import os
from pathlib import Path
import subprocess
import sys

from dags.utils.config import SCHEDULES


ROOT = Path(__file__).resolve().parents[3]
AIRFLOW_SERVICES = ("airflow-init", "airflow-scheduler", "airflow-webserver")
SHARED_COMPOSE = ROOT / "compose.yaml"
ISOLATED_COMPOSE = ROOT / "deploy/espn/airflow.compose.yaml"
CORE_CATALOG_URL = (
    "https://sports.core.api.espn.com/v2/sports/soccer/leagues"
    "?limit=500&lang=en&region=us"
)


def _render_compose(
    *,
    compose_file: Path = SHARED_COMPOSE,
    frozen_ref: tuple[str, str] | None = None,
    extra_environment: dict[str, str] | None = None,
    profiles: tuple[str, ...] = (),
) -> dict:
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
            "ESPN_RELEASE_COMMIT": "a" * 40,
            "ESPN_RELEASE_TREE_SHA256": "b" * 64,
            "ESPN_BRONZE_LAYOUT_MODE": "legacy14",
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
    if extra_environment is not None:
        environment.update(extra_environment)
    command = [
        "docker",
        "compose",
        "--env-file",
        str(ROOT / ".env.example"),
        "-f",
        str(compose_file),
    ]
    for profile in profiles:
        command.extend(("--profile", profile))
    command.extend(
        [
            "config",
            "--format",
            "json",
        ]
    )
    result = subprocess.run(
        command,
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _isolated_environment() -> dict[str, str]:
    return {
        "ESPN_AIRFLOW_IMAGE": "airflow@example.test/espn@sha256:" + "1" * 64,
        "ESPN_POSTGRES_IMAGE": "postgres@example.test/espn@sha256:" + "2" * 64,
        "ESPN_RELEASE_ROOT": "/tmp/espn-release",
        "ESPN_DAGBAG_ROOT": "/tmp/espn-dagbag",
        "ESPN_RELEASE_COMMIT": "a" * 40,
        "ESPN_RELEASE_TREE_SHA256": "b" * 64,
        "ESPN_BRONZE_LAYOUT_MODE": "legacy14",
        "ESPN_AIRFLOW_DB_PASSWORD": "compose-test",
        "ESPN_AIRFLOW_DATABASE_URL": (
            "postgresql+psycopg2://airflow:compose-test@airflow-metadb:5432/airflow"
        ),
        "ESPN_CONTROL_DATABASE_URL": "postgresql://espn-control.example.test/espn",
        "AIRFLOW__CORE__FERNET_KEY": "compose-test-fernet",
        "AIRFLOW__WEBSERVER__SECRET_KEY": "compose-test-web",
        "ICEBERG_WAREHOUSE": "football",
        "TRINO_PASSWORD": "compose-test",
        "S3_ACCESS_KEY": "compose-test",
        "S3_SECRET_KEY": "compose-test",
    }


def test_shared_stack_keeps_espn_child_trigger_only_for_compatibility() -> None:
    """The shared-stack compatibility path never schedules the child itself."""
    assert SCHEDULES["dag_ingest_espn"] is None
    assert SCHEDULES["dag_master_pipeline"] == "0 14 * * *"


def test_shared_compose_requires_exact_espn_release_identity() -> None:
    source = SHARED_COMPOSE.read_text(encoding="utf-8")

    assert "ESPN_RELEASE_COMMIT: ${ESPN_RELEASE_COMMIT:?" in source
    assert "ESPN_RELEASE_TREE_SHA256: ${ESPN_RELEASE_TREE_SHA256:?" in source


def test_every_runtime_compose_requires_an_explicit_espn_layout_mode() -> None:
    """A process must never silently fall back to the legacy physical layout."""
    required = (
        "ESPN_BRONZE_LAYOUT_MODE: ${ESPN_BRONZE_LAYOUT_MODE:?"
        "set exact ESPN Bronze layout mode (legacy14 or compact6)}"
    )

    for compose_file in (SHARED_COMPOSE, ISOLATED_COMPOSE):
        assert required in compose_file.read_text(encoding="utf-8")

    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "# ESPN_BRONZE_LAYOUT_MODE=legacy14" in example


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
        assert environment["ESPN_BRONZE_LAYOUT_MODE"] == "legacy14"
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


def test_isolated_projection_builder_emits_only_reviewed_espn_dag_roots(
    tmp_path,
) -> None:
    output = tmp_path / "espn-dagbag"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/build_espn_dagbag_projection.py"),
            "--release-root",
            str(ROOT),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert {path.name for path in output.iterdir()} == {
        ".airflowignore",
        "dag_backfill_espn.py",
        "dag_discover_espn_registry.py",
        "dag_ingest_espn.py",
        "dag_monitor_espn.py",
        "dag_repair_espn.py",
        "dag_replay_espn.py",
        "dag_trigger_espn_daily.py",
        "scripts",
        "utils",
    }
    assert not (output / "dag_master_pipeline.py").exists()
    for name in (
        "dag_backfill_espn.py",
        "dag_discover_espn_registry.py",
        "dag_ingest_espn.py",
        "dag_monitor_espn.py",
        "dag_repair_espn.py",
        "dag_replay_espn.py",
        "dag_trigger_espn_daily.py",
    ):
        assert (output / name).readlink() == Path(f"/opt/espn-source/dags/{name}")
    assert (output / "utils").readlink() == Path("/opt/espn-source/dags/utils")
    assert (output / "scripts").readlink() == Path("/opt/espn-source/dags/scripts")
    assert (output / ".airflowignore").read_bytes() == (
        ROOT / "configs/espn/isolated.airflowignore"
    ).read_bytes()
    projection_snapshot = {
        path.name: (
            ("symlink", str(path.readlink()))
            if path.is_symlink()
            else ("file", path.read_bytes())
        )
        for path in output.iterdir()
    }
    repeated = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts/build_espn_dagbag_projection.py"),
            "--release-root",
            str(ROOT),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert repeated.returncode != 0
    assert "projection output must be a new path" in repeated.stderr
    assert projection_snapshot == {
        path.name: (
            ("symlink", str(path.readlink()))
            if path.is_symlink()
            else ("file", path.read_bytes())
        )
        for path in output.iterdir()
    }


def test_rendered_isolated_espn_compose_proves_role_projection_and_freeze() -> None:
    unfrozen = _render_compose(
        compose_file=ISOLATED_COMPOSE,
        extra_environment=_isolated_environment(),
        profiles=("ui",),
    )["services"]
    for name in AIRFLOW_SERVICES:
        environment = unfrozen[name]["environment"]
        assert environment.get("ESPN_DISCOVERY_STATE_REF_URI") is None
        assert environment.get("ESPN_DISCOVERY_STATE_REF_SHA256") is None

    frozen_ref = (
        "s3://football/artifacts/espn/discovery/run/discovery-state.json",
        "a" * 64,
    )
    rendered = _render_compose(
        compose_file=ISOLATED_COMPOSE,
        frozen_ref=frozen_ref,
        extra_environment=_isolated_environment(),
        profiles=("ui",),
    )
    services = rendered["services"]
    assert set(services) == {
        "airflow-init",
        "airflow-metadb",
        "airflow-scheduler",
        "airflow-webserver",
    }

    for name in AIRFLOW_SERVICES:
        service = services[name]
        environment = service["environment"]
        assert environment["ESPN_ISOLATED_STACK"] == "1"
        assert environment["ESPN_BRONZE_LAYOUT_MODE"] == "legacy14"
        assert environment["ESPN_DISCOVERY_CATALOG_URL"] == CORE_CATALOG_URL
        assert environment["ESPN_DISCOVERY_STATE_REF_URI"] == frozen_ref[0]
        assert environment["ESPN_DISCOVERY_STATE_REF_SHA256"] == frozen_ref[1]
        assert environment["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] == (
            "postgresql+psycopg2://airflow:compose-test@airflow-metadb:5432/airflow"
        )
        assert environment["ESPN_CONTROL_DATABASE_URL"] == (
            "postgresql://espn-control.example.test/espn"
        )
        assert (
            environment["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"]
            != environment["ESPN_CONTROL_DATABASE_URL"]
        )
        assert set(service["networks"]) == {"backend", "default", "storage"}
        volumes = {volume["target"]: volume for volume in service["volumes"]}
        assert volumes["/opt/airflow/dags"]["type"] == "bind"
        assert volumes["/opt/airflow/dags"]["source"] == "/tmp/espn-dagbag"
        assert volumes["/opt/airflow/dags"]["read_only"] is True
        assert volumes["/opt/espn-source/dags"]["source"] == ("/tmp/espn-release/dags")
        assert volumes["/opt/espn-source/dags"]["read_only"] is True

    init_command = services["airflow-init"]["command"][-1]
    topology_preflight = "python /opt/airflow/scripts/verify_espn_database_topology.py"
    assert topology_preflight in init_command
    assert "airflow db migrate" in init_command
    assert init_command.index(topology_preflight) < init_command.index(
        "airflow db migrate"
    )
    assert "airflow pools set espn_http_pool 1" in init_command
    assert "airflow pools set espn_repository_pool 16" in init_command
    for dag_id in (
        "dag_ingest_espn",
        "dag_repair_espn",
        "dag_backfill_espn",
        "dag_replay_espn",
        "dag_discover_espn_registry",
        "dag_monitor_espn",
        "dag_trigger_espn_daily",
    ):
        assert dag_id in init_command
    assert "dag_master_pipeline" not in init_command
    assert services["airflow-scheduler"]["depends_on"]["airflow-init"] == {
        "condition": "service_completed_successfully",
        "required": True,
    }
    metadb = services["airflow-metadb"]
    assert metadb["environment"] == {
        "POSTGRES_DB": "airflow",
        "POSTGRES_PASSWORD": "compose-test",
        "POSTGRES_USER": "airflow",
    }
    assert set(metadb["networks"]) == {"default"}
    assert rendered["volumes"]["espn_airflow_metadata"]["name"] == (
        "espn-airflow_espn_airflow_metadata"
    )
