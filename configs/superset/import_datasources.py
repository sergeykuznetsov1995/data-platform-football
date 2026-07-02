#!/usr/bin/env python
# =============================================================================
# Superset datasources importer (Superset 4.x)
# =============================================================================
# Читает configs/superset/datasources.yaml и создаёт/обновляет database +
# datasets через нативные модели Superset (SQLAlchemy session из app context).
#
# Почему Python, а не CLI:
#   В Superset 4.x официальный `superset import_datasources` принимает только
#   ZIP-архив со специфической структурой (metadata.yaml + databases/*.yaml +
#   datasets/*.yaml). Собирать ZIP вручную из плоского YAML — лишний шаг,
#   проще обратиться к моделям напрямую.
#
# Идемпотентность:
#   - database по `database_name` ищется и обновляется на месте (URI, extra, ...)
#   - dataset по (database_id, schema, table_name) — то же самое.
#   - Метрики/колонки Superset auto-discover при первом запросе — не трогаем.
#
# Запуск (из bootstrap.sh):
#   python /app/pythonpath/import_datasources.py /app/pythonpath/datasources.yaml
# =============================================================================
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import yaml

# pylint: disable=import-error  # imports resolved at runtime inside Superset image
# NOTE: superset.connectors.sqla.models / superset.models.core touch
# `current_app.config` at MODULE-IMPORT time, so importing them before
# create_app() raises "Working outside of application context". We defer
# those heavy imports into main() (after app_context push). Only the lightweight
# `create_app` is imported here.
from superset.app import create_app  # type: ignore

logging.basicConfig(
    level=os.environ.get("SUPERSET_LOG_LEVEL", "INFO"),
    format="[superset-import] %(levelname)s %(message)s",
)
log = logging.getLogger("import_datasources")


def _resolve_uri(template_uri: str) -> str:
    """Подставить пароль superset из env TRINO_SUPERSET_PASSWORD.

    Шаблон в YAML: `trino://superset@trino:8443/iceberg`.
    Если пароля нет в env — оставляем шаблон как есть (Superset потом не сможет
    подключиться, но это явная ошибка конфигурации, а не молчаливая).
    """
    password = os.environ.get("TRINO_SUPERSET_PASSWORD")
    if not password:
        log.warning(
            "TRINO_SUPERSET_PASSWORD is empty; database URI will have no password"
        )
        return template_uri
    # Простая подстановка: 'user@' -> 'user:pwd@'. Достаточно для нашего шаблона.
    if "@" in template_uri and ":" not in template_uri.split("//", 1)[1].split("@", 1)[0]:
        scheme, rest = template_uri.split("//", 1)
        userinfo, host_part = rest.split("@", 1)
        return f"{scheme}//{userinfo}:{password}@{host_part}"
    return template_uri


def _upsert_database(spec: dict[str, Any]) -> Database:
    name = spec["database_name"]
    uri = _resolve_uri(spec["sqlalchemy_uri"])

    extra = spec.get("extra", "{}")
    if isinstance(extra, dict):
        extra_str = json.dumps(extra)
    else:
        # YAML literal block — это уже строка с JSON; валидируем парсингом.
        json.loads(extra)
        extra_str = extra

    existing = db.session.query(Database).filter_by(database_name=name).one_or_none()
    if existing is None:
        log.info("creating database '%s'", name)
        existing = Database(database_name=name)
        db.session.add(existing)

    existing.sqlalchemy_uri = uri
    existing.expose_in_sqllab = bool(spec.get("expose_in_sqllab", True))
    existing.allow_ctas = bool(spec.get("allow_ctas", False))
    existing.allow_cvas = bool(spec.get("allow_cvas", False))
    existing.allow_dml = bool(spec.get("allow_dml", False))
    existing.allow_run_async = bool(spec.get("allow_run_async", True))
    existing.cache_timeout = spec.get("cache_timeout")
    existing.extra = extra_str
    # SQL Lab/чарты выполняются под именем залогиненного юзера (X-Trino-User);
    # права режет Trino: rules.json + groups.txt (фаза 7)
    existing.impersonate_user = bool(spec.get("impersonate_user", False))

    db.session.commit()
    log.info("database '%s' upserted (id=%s)", name, existing.id)
    return existing


def _upsert_dataset(database: Database, table_spec: dict[str, Any]) -> None:
    table_name = table_spec["table_name"]
    schema = table_spec.get("schema")
    description = (table_spec.get("description") or "").strip()
    cache_timeout = table_spec.get("cache_timeout")
    default_endpoint = table_spec.get("default_endpoint")

    existing = (
        db.session.query(SqlaTable)
        .filter_by(database_id=database.id, schema=schema, table_name=table_name)
        .one_or_none()
    )
    if existing is None:
        log.info("creating dataset %s.%s", schema, table_name)
        existing = SqlaTable(
            database_id=database.id,
            schema=schema,
            table_name=table_name,
        )
        db.session.add(existing)

    if existing.description != description and description:
        existing.description = description
    if cache_timeout is not None:
        existing.cache_timeout = cache_timeout
    if default_endpoint is not None:
        existing.default_endpoint = default_endpoint

    db.session.commit()
    log.info("dataset %s.%s upserted (id=%s)", schema, table_name, existing.id)


def main(yaml_path: Path) -> int:
    if not yaml_path.exists():
        log.error("datasources YAML not found: %s", yaml_path)
        return 2

    with yaml_path.open("r", encoding="utf-8") as fh:
        spec = yaml.safe_load(fh)

    if not isinstance(spec, dict) or "databases" not in spec:
        log.error("invalid YAML: top-level 'databases' key missing")
        return 2

    app = create_app()
    with app.app_context():
        # Deferred imports — require active app context (see module header).
        # pylint: disable=import-outside-toplevel
        global db, security_manager, SqlaTable, Database  # noqa: PLW0603
        from superset import db, security_manager  # type: ignore  # noqa: F401
        from superset.connectors.sqla.models import SqlaTable  # type: ignore  # noqa: F401
        from superset.models.core import Database  # type: ignore  # noqa: F401

        # Гарантируем существование security manager (создаст роли при первом запуске)
        _ = security_manager
        for db_spec in spec["databases"]:
            database = _upsert_database(db_spec)
            for tbl in db_spec.get("tables", []):
                _upsert_dataset(database, tbl)

    log.info("done")
    return 0


if __name__ == "__main__":
    path = Path(sys.argv[1] if len(sys.argv) > 1 else "/app/pythonpath/datasources.yaml")
    sys.exit(main(path))
