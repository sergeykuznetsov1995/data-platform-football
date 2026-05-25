#!/usr/bin/env python
# =============================================================================
# Superset dashboards importer (Superset 4.x)
# =============================================================================
# Идемпотентный импортёр декларативных дашбордов из configs/superset/dashboards/.
# Каждый файл dashboards/<name>.py определяет переменную DASHBOARD типа
# Dashboard (см. ниже). Импортёр находит дашборд по slug и обновляет/создаёт.
#
# Почему не через Superset import-ZIP CLI:
#   В 4.x официальный импорт принимает ZIP с metadata.yaml + charts/* + dashboards/*.
#   Сборка ZIP из flat-описаний — лишний шаг; ходим прямиком к моделям.
#
# Логика идемпотентности — по slug:
#   - найден → update title/description/json_metadata + переаттач datasets
#   - нет    → создаём новую Dashboard.
#
# Charts (slices) пока создаются как заглушки: импортёр гарантирует, что для
# каждого spec'а есть Slice с именем `<dashboard_slug>__<slice_name>` и
# datasource_id. Полный layout/viz_type конфиг прокидывается в
# `slice.params` как JSON; Superset рендерит из этого. Это minimum viable —
# полноценный layout JSON генерируется в T8.
#
# Запуск:
#   python /app/pythonpath/import_dashboards.py
# =============================================================================
from __future__ import annotations

import importlib.util
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# pylint: disable=import-error  # imports resolved at runtime inside Superset image
from superset import db, security_manager  # type: ignore
from superset.app import create_app  # type: ignore
from superset.connectors.sqla.models import SqlaTable  # type: ignore
from superset.models.dashboard import Dashboard as SsDashboard  # type: ignore
from superset.models.slice import Slice  # type: ignore

logging.basicConfig(
    level=os.environ.get("SUPERSET_LOG_LEVEL", "INFO"),
    format="[superset-dash] %(levelname)s %(message)s",
)
log = logging.getLogger("import_dashboards")


@dataclass
class Dashboard:
    """Декларативное описание дашборда.

    charts — список dict, каждый с обязательными ключами:
      - slice_name: str (уникален в рамках дашборда)
      - viz_type: str (table/bar/line/scatter/heatmap/...)
      - dataset:   str (table_name из datasources.yaml; schema=gold)
      - params:    dict (Superset slice params; metrics/groupby/etc)
    """

    slug: str
    title: str
    description: str
    datasets: list[str]
    charts: list[dict[str, Any]] = field(default_factory=list)


def _find_dataset(table_name: str, schema: str = "gold") -> SqlaTable:
    ds = (
        db.session.query(SqlaTable)
        .filter_by(schema=schema, table_name=table_name)
        .one_or_none()
    )
    if ds is None:
        raise RuntimeError(
            f"dataset {schema}.{table_name} not found — run import_datasources first"
        )
    return ds


def _upsert_slice(dash_slug: str, spec: dict[str, Any]) -> Slice:
    name = f"{dash_slug}__{spec['slice_name']}"
    ds = _find_dataset(spec["dataset"])
    existing = db.session.query(Slice).filter_by(slice_name=name).one_or_none()
    if existing is None:
        log.info("creating slice '%s'", name)
        existing = Slice(slice_name=name)
        db.session.add(existing)
    existing.viz_type = spec["viz_type"]
    existing.datasource_type = "table"
    existing.datasource_id = ds.id
    existing.params = json.dumps(spec.get("params", {}))
    db.session.commit()
    return existing


def _upsert_dashboard(dash: Dashboard) -> None:
    existing = db.session.query(SsDashboard).filter_by(slug=dash.slug).one_or_none()
    if existing is None:
        log.info("creating dashboard '%s'", dash.slug)
        existing = SsDashboard(slug=dash.slug)
        db.session.add(existing)
    existing.dashboard_title = dash.title
    existing.description = dash.description
    existing.json_metadata = json.dumps({"datasets": dash.datasets})
    slices = [_upsert_slice(dash.slug, c) for c in dash.charts]
    existing.slices = slices
    db.session.commit()
    log.info("dashboard '%s' upserted (id=%s, slices=%d)", dash.slug, existing.id, len(slices))


DASHBOARDS: list[Dashboard] = []


def _discover_dashboards(dash_dir: Path) -> list[Dashboard]:
    found: list[Dashboard] = []
    for py in sorted(dash_dir.glob("*.py")):
        if py.name.startswith("_"):
            continue
        spec = importlib.util.spec_from_file_location(py.stem, py)
        if spec is None or spec.loader is None:
            continue
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        dash = getattr(mod, "DASHBOARD", None)
        if isinstance(dash, Dashboard):
            found.append(dash)
        else:
            log.warning("%s: no DASHBOARD variable, skipped", py.name)
    return found


def main() -> int:
    dash_dir = Path(__file__).parent / "dashboards"
    if not dash_dir.exists():
        log.error("dashboards dir not found: %s", dash_dir)
        return 2
    app = create_app()
    with app.app_context():
        _ = security_manager
        dashboards = DASHBOARDS + _discover_dashboards(dash_dir)
        for dash in dashboards:
            _upsert_dashboard(dash)
    log.info("done (%d dashboards)", len(dashboards))
    return 0


if __name__ == "__main__":
    sys.exit(main())
