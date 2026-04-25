#!/usr/bin/env python
# =============================================================================
# OpenMetadata: apply table/column descriptions + tags from YAML
# =============================================================================
# Читает все configs/openmetadata/descriptions/*.yaml и применяет
# table-level + per-column descriptions и tags через OpenMetadata Python SDK.
#
# Запуск:
#   docker compose exec openmetadata-ingestion python /opt/configs/apply_descriptions.py
#
# Аутентификация (env):
#   OM_HOST       — http://openmetadata-server:8585/api (default)
#   OM_JWT_TOKEN  — bot JWT, сгенерированный в OM UI после первого логина
#                   (Settings -> Bots -> ingestion-bot -> Generate Token)
#
# Идемпотентность:
#   Перед PATCH сравнивает текущее значение с целевым; одинаковое — skip.
#   `relationships:` сейчас не применяются (OM не поддерживает явные FK через
#   API — связь выводится через lineage). Секция оставлена как декларативная
#   документация и для будущего экспорта в DataHub / другой каталог.
# =============================================================================
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

import yaml

# pylint: disable=import-error  # Resolved at runtime inside openmetadata-ingestion image
from metadata.generated.schema.entity.data.table import Column, Table  # type: ignore
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (  # type: ignore
    AuthProvider,
    OpenMetadataConnection,
)
from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (  # type: ignore
    OpenMetadataJWTClientConfig,
)
from metadata.generated.schema.type.tagLabel import (  # type: ignore
    LabelType,
    State,
    TagLabel,
    TagSource,
)
from metadata.ingestion.ometa.ometa_api import OpenMetadata  # type: ignore

logging.basicConfig(
    level=os.environ.get("OM_LOG_LEVEL", "INFO"),
    format="[om-apply] %(levelname)s %(message)s",
)
log = logging.getLogger("apply_descriptions")

DESCRIPTIONS_DIR = Path(
    os.environ.get(
        "OM_DESCRIPTIONS_DIR",
        "/opt/configs/descriptions",
    )
)


# -----------------------------------------------------------------------------
# OpenMetadata client
# -----------------------------------------------------------------------------
def _build_client() -> OpenMetadata:
    host = os.environ.get("OM_HOST", "http://openmetadata-server:8585/api")
    token = os.environ.get("OM_JWT_TOKEN", "").strip()
    if not token:
        log.error(
            "OM_JWT_TOKEN is empty. Generate it at OM UI -> Settings -> Bots "
            "-> ingestion-bot -> Generate Token, then export OM_JWT_TOKEN."
        )
        sys.exit(2)

    conn = OpenMetadataConnection(
        hostPort=host,
        authProvider=AuthProvider.openmetadata,
        securityConfig=OpenMetadataJWTClientConfig(jwtToken=token),
    )
    client = OpenMetadata(conn)
    if not client.health_check():
        log.error("OpenMetadata health check failed at %s", host)
        sys.exit(3)
    log.info("connected to OpenMetadata at %s", host)
    return client


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _to_tag_labels(tag_names: list[str] | None) -> list[TagLabel]:
    if not tag_names:
        return []
    labels: list[TagLabel] = []
    for name in tag_names:
        labels.append(
            TagLabel(
                tagFQN=name,
                labelType=LabelType.Manual,
                state=State.Confirmed,
                source=TagSource.Classification,
            )
        )
    return labels


def _column_index(table: Table) -> dict[str, Column]:
    return {col.name.root if hasattr(col.name, "root") else str(col.name): col for col in table.columns}


def _apply_table(client: OpenMetadata, spec: dict[str, Any]) -> None:
    table_spec = spec.get("table") or {}
    fqn = table_spec.get("fullyQualifiedName")
    if not fqn:
        log.warning("missing table.fullyQualifiedName; skipping")
        return

    target_desc = (table_spec.get("description") or "").strip()
    target_tags = _to_tag_labels(table_spec.get("tags"))

    table: Table | None = client.get_by_name(entity=Table, fqn=fqn, fields=["tags", "columns"])
    if table is None:
        log.warning("table not found in OM (still being ingested?): %s", fqn)
        return

    changed = False

    if target_desc and (table.description is None or table.description.root != target_desc):
        log.info("PATCH description: %s", fqn)
        client.patch_description(entity=Table, source=table, description=target_desc, force=True)
        changed = True

    if target_tags:
        existing_fqns = {t.tagFQN for t in (table.tags or [])}
        target_fqns = {t.tagFQN for t in target_tags}
        if target_fqns - existing_fqns:
            log.info("PATCH tags %s -> %s", fqn, sorted(target_fqns - existing_fqns))
            client.patch_tags(entity=Table, source=table, tag_labels=target_tags)
            changed = True

    # Per-column descriptions + tags
    by_name = _column_index(table)
    for col_spec in spec.get("columns", []):
        col_name = col_spec.get("name")
        col = by_name.get(col_name)
        if col is None:
            log.warning("  column not found in table %s: %s", fqn, col_name)
            continue

        col_desc = (col_spec.get("description") or "").strip()
        if col_desc and (col.description is None or col.description.root != col_desc):
            log.info("  PATCH column description: %s.%s", fqn, col_name)
            client.patch_column_description(
                table=table,
                column_fqn=f"{fqn}.{col_name}",
                description=col_desc,
                force=True,
            )
            changed = True

        col_tags = _to_tag_labels(col_spec.get("tags"))
        if col_tags:
            existing = {t.tagFQN for t in (col.tags or [])}
            wanted = {t.tagFQN for t in col_tags}
            if wanted - existing:
                log.info("  PATCH column tags: %s.%s += %s", fqn, col_name, sorted(wanted - existing))
                client.patch_column_tag(
                    table=table,
                    column_fqn=f"{fqn}.{col_name}",
                    tag_label=col_tags[0],
                )
                changed = True

    if not changed:
        log.info("no changes for %s", fqn)


def main() -> int:
    if not DESCRIPTIONS_DIR.exists():
        log.error("descriptions dir not found: %s", DESCRIPTIONS_DIR)
        return 2

    client = _build_client()

    yaml_files = sorted(DESCRIPTIONS_DIR.glob("*.yaml"))
    if not yaml_files:
        log.warning("no YAML files in %s", DESCRIPTIONS_DIR)
        return 0

    for path in yaml_files:
        log.info("processing %s", path.name)
        with path.open("r", encoding="utf-8") as fh:
            spec = yaml.safe_load(fh)
        if not isinstance(spec, dict):
            log.warning("skip %s: not a mapping", path.name)
            continue
        try:
            _apply_table(client, spec)
        except Exception as exc:  # pylint: disable=broad-except
            log.exception("error applying %s: %s", path.name, exc)

    log.info("apply_descriptions done; processed %d files", len(yaml_files))
    return 0


if __name__ == "__main__":
    sys.exit(main())
