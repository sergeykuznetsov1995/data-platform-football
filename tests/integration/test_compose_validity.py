"""
Smoke tests for ``compose.yaml`` and BI/Catalog YAML configs.

These tests confirm the **shape** of the compose file and supporting YAML
configs without actually starting any container. They run on the host
where docker is present (CI + dev workstations).

Marker policy:
  * ``docker compose config`` checks need the docker CLI → ``integration``.
  * Pure YAML structural checks are fast → ``unit``.

If docker is missing the ``compose config`` test is skipped (so PRs from
forks without docker still pass the rest of the suite).
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = PROJECT_ROOT / "compose.yaml"
SUPERSET_DIR = PROJECT_ROOT / "configs" / "superset"
OPENMETADATA_DIR = PROJECT_ROOT / "configs" / "openmetadata"
DESCRIPTIONS_DIR = OPENMETADATA_DIR / "descriptions"


# ---------------------------------------------------------------------------
# 1. docker compose config — syntax + interpolation
# ---------------------------------------------------------------------------


def _docker_available() -> bool:
    return shutil.which("docker") is not None


def _compose_config_json() -> dict:
    """Run `docker compose config --format json` and return parsed dict.

    Skips the test if docker is missing or compose is unavailable.
    """
    if not _docker_available():
        pytest.skip("docker CLI not available on this host")

    proc = subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "config", "--format", "json"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"docker compose config failed (rc={proc.returncode}):\n"
            f"STDOUT: {proc.stdout[:1000]}\nSTDERR: {proc.stderr[:1000]}"
        )
    return json.loads(proc.stdout)


@pytest.mark.integration
class TestComposeFile:
    def test_compose_file_exists(self):
        assert COMPOSE_FILE.exists(), f"compose file not found: {COMPOSE_FILE}"

    def test_compose_syntax_valid(self):
        """``docker compose config --quiet`` exit 0 ⇒ YAML + interpolation OK."""
        if not _docker_available():
            pytest.skip("docker CLI not available")
        proc = subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "config", "--quiet"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert proc.returncode == 0, (
            f"compose validation failed:\n{proc.stderr[:2000]}"
        )

    def test_required_bi_catalog_services_present(self):
        cfg = _compose_config_json()
        services = cfg.get("services", {})
        required = {
            "superset",
            "superset-worker",
            "superset-beat",
            "elasticsearch",
            "openmetadata-server",
            "openmetadata-ingestion",
        }
        missing = required - set(services.keys())
        assert not missing, f"Missing services in compose: {missing}"

    def test_superset_exposes_8088(self):
        cfg = _compose_config_json()
        ports = cfg["services"]["superset"].get("ports", []) or []
        published = [str(p.get("published")) for p in ports if isinstance(p, dict)]
        assert "8088" in published, (
            f"superset must publish port 8088 (found published={published})"
        )

    def test_superset_dependencies(self):
        cfg = _compose_config_json()
        deps = cfg["services"]["superset"].get("depends_on", {}) or {}
        # depends_on may be list or dict (compose v3+ short/long form)
        dep_names = set(deps) if isinstance(deps, dict) else set(deps)
        for required in ("postgres", "redis", "trino"):
            assert required in dep_names, (
                f"superset must depend_on '{required}' (got {sorted(dep_names)})"
            )

    def test_elasticsearch_no_published_ports(self):
        """Security: ES must NOT expose 9200 to the host (HDFS-like internal only)."""
        cfg = _compose_config_json()
        es = cfg["services"]["elasticsearch"]
        ports = es.get("ports") or []
        # Allow empty list or absent key. Anything else is a leak.
        assert ports == [] or ports is None, (
            f"elasticsearch must NOT publish ports (got {ports})"
        )

    @pytest.mark.parametrize(
        "service",
        [
            "superset",
            "superset-worker",
            "superset-beat",
            "elasticsearch",
            "openmetadata-server",
            "openmetadata-ingestion",
        ],
    )
    def test_service_has_mem_limit(self, service):
        cfg = _compose_config_json()
        svc = cfg["services"][service]
        # Compose normalises mem_limit into deploy.resources.limits.memory
        deploy = svc.get("deploy", {}) or {}
        limits = (deploy.get("resources") or {}).get("limits") or {}
        memory_limit = svc.get("mem_limit") or limits.get("memory")
        assert memory_limit, (
            f"service '{service}' must declare mem_limit (got {svc.get('mem_limit')})"
        )


@pytest.mark.unit
class TestSofaScoreRegistryMount:
    """Airflow consumes activation state but must never mutate the registry."""

    @staticmethod
    def _airflow_services() -> list[dict]:
        with COMPOSE_FILE.open("r", encoding="utf-8") as fh:
            services = yaml.safe_load(fh)["services"]
        return [
            services[name]
            for name in ("airflow-init", "airflow-scheduler", "airflow-webserver")
        ]

    def test_registry_path_is_explicit_in_all_airflow_services(self):
        for service in self._airflow_services():
            assert service["environment"]["SOFASCORE_REGISTRY_PATH"] == (
                "/opt/airflow/configs/sofascore/tournaments.json"
            )

    def test_registry_directory_is_mounted_read_only(self):
        for service in self._airflow_services():
            assert (
                "./configs/sofascore:/opt/airflow/configs/sofascore:ro"
                in service["volumes"]
            )


# ---------------------------------------------------------------------------
# 2. Superset datasources YAML
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSupersetDatasourcesYaml:
    def test_file_exists(self):
        assert (SUPERSET_DIR / "datasources.yaml").exists()

    def test_parses(self):
        with (SUPERSET_DIR / "datasources.yaml").open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        assert isinstance(data, dict)
        assert "databases" in data
        assert isinstance(data["databases"], list)
        assert data["databases"], "datasources.yaml has no databases"

    def test_first_database_minimal_shape(self):
        with (SUPERSET_DIR / "datasources.yaml").open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        db = data["databases"][0]
        assert "database_name" in db
        assert "sqlalchemy_uri" in db
        # Tables list may be empty, but the key should exist
        assert "tables" in db

    def test_extra_field_is_valid_json(self):
        """``extra:`` is a YAML literal block string; must parse as JSON."""
        with (SUPERSET_DIR / "datasources.yaml").open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        for db in data["databases"]:
            extra = db.get("extra")
            if extra is None:
                continue
            if isinstance(extra, str):
                json.loads(extra)  # raises if invalid


# ---------------------------------------------------------------------------
# 3. OpenMetadata description YAMLs
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestOpenMetadataDescriptions:
    def test_descriptions_dir_exists(self):
        assert DESCRIPTIONS_DIR.is_dir()

    def test_at_least_one_yaml_present(self):
        yamls = list(DESCRIPTIONS_DIR.glob("*.yaml"))
        assert yamls, f"no YAML files in {DESCRIPTIONS_DIR}"

    @pytest.mark.parametrize(
        "path",
        sorted(p.name for p in DESCRIPTIONS_DIR.glob("*.yaml"))
        if DESCRIPTIONS_DIR.exists()
        else [],
    )
    def test_each_yaml_parses_and_has_table_fqn(self, path):
        full = DESCRIPTIONS_DIR / path
        with full.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        assert isinstance(data, dict), f"{path}: top-level must be mapping"
        table = data.get("table") or {}
        assert "fullyQualifiedName" in table, (
            f"{path}: missing table.fullyQualifiedName"
        )
        # FQN should follow the trino_iceberg.iceberg.<schema>.<table> pattern
        fqn = table["fullyQualifiedName"]
        assert fqn.startswith("trino_iceberg."), (
            f"{path}: unexpected FQN prefix: {fqn}"
        )


# ---------------------------------------------------------------------------
# 4. OpenMetadata ingestion / lineage YAMLs
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestOpenMetadataIngestionYaml:
    @pytest.mark.parametrize(
        "filename", ["trino_ingestion.yaml", "trino_lineage.yaml"]
    )
    def test_file_parses_with_required_top_level_keys(self, filename):
        path = OPENMETADATA_DIR / filename
        assert path.exists(), f"missing {path}"
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        assert isinstance(data, dict)
        for required in ("source", "sink", "workflowConfig"):
            assert required in data, f"{filename}: missing top-level '{required}'"

    def test_ingestion_targets_iceberg_catalog(self):
        path = OPENMETADATA_DIR / "trino_ingestion.yaml"
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        cfg = data["source"]["serviceConnection"]["config"]
        assert cfg["catalog"] == "iceberg"
        # Filters must include bronze/silver/gold; do not assume order.
        includes = cfg.get("__unused__")  # we'll look further down
        # Real path is under sourceConfig
        schema_inc = data["source"]["sourceConfig"]["config"]["schemaFilterPattern"][
            "includes"
        ]
        assert any("bronze" in s for s in schema_inc)
        assert any("silver" in s for s in schema_inc)
        assert any("gold" in s for s in schema_inc)


# ---------------------------------------------------------------------------
# 5. OpenMetadata auth env — break-glass defaults (#866)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestOpenMetadataAuthEnv:
    """SSO включается только через .env; пустой .env обязан давать basic-режим.

    Дефолты ${VAR:-…} в anchor'е — это break-glass: закомментировал OM_AUTH_*
    в .env → OM вернулся к basic-логину, бот-JWT продолжает валидироваться.
    """

    OM_SERVICES = ("openmetadata-migrate", "openmetadata-server")

    @staticmethod
    def _services() -> dict:
        with COMPOSE_FILE.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh)["services"]

    def test_break_glass_defaults_are_basic_mode(self):
        # (env-ключ, дефолт при пустом .env) — дефолты = сегодняшний basic-режим
        expected = {
            "AUTHENTICATION_PROVIDER": "${OM_AUTH_PROVIDER:-basic}",
            "AUTHENTICATION_CLIENT_TYPE": "${OM_AUTH_CLIENT_TYPE:-public}",
            "OIDC_CLIENT_SECRET": "${OPENMETADATA_OIDC_CLIENT_SECRET:-}",
            "AUTHORIZER_ADMIN_PRINCIPALS": "${OM_ADMIN_PRINCIPALS:-[admin]}",
        }
        services = self._services()
        for name in self.OM_SERVICES:
            env = services[name]["environment"]
            for key, raw in expected.items():
                assert env[key] == raw, f"{name}.{key}: {env.get(key)!r}"

    def test_public_keys_default_keeps_om_own_jwks(self):
        # Иначе break-glass/дефолтный запуск ломает валидацию бот-JWT.
        services = self._services()
        for name in self.OM_SERVICES:
            keys = services[name]["environment"]["AUTHENTICATION_PUBLIC_KEYS"]
            assert "api/v1/system/config/jwks" in keys, f"{name}: {keys!r}"

    def test_jwt_key_paths_default_to_image_bundled_keys(self):
        # Пустой .env = ключи образа (dev-стек поднимается без подготовки);
        # прод обязан переопределить OM_RSA_* (см. gen_om_jwt_keys.sh).
        services = self._services()
        for name in self.OM_SERVICES:
            env = services[name]["environment"]
            assert env["RSA_PUBLIC_KEY_FILE_PATH"] == (
                "${OM_RSA_PUBLIC_KEY_FILE_PATH:-./conf/public_key.der}"
            )
            assert env["RSA_PRIVATE_KEY_FILE_PATH"] == (
                "${OM_RSA_PRIVATE_KEY_FILE_PATH:-./conf/private_key.der}"
            )

    def test_jwtkeys_volume_mounted_read_only_in_both_services(self):
        # migrate парсит тот же openmetadata.yaml → ключи нужны обоим.
        services = self._services()
        for name in self.OM_SERVICES:
            assert (
                "./configs/openmetadata/jwtkeys:/etc/openmetadata/jwtkeys:ro"
                in services[name].get("volumes", [])
            ), f"{name}: jwtkeys volume missing"

    def test_ingestion_bot_principal_untouched(self):
        services = self._services()
        for name in self.OM_SERVICES:
            env = services[name]["environment"]
            assert env["AUTHORIZER_INGESTION_PRINCIPALS"] == "[ingestion-bot]"
