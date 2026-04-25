#!/usr/bin/env python
# =============================================================================
# Superset dashboard: "Team form overview"
# =============================================================================
# Декларативное описание дашборда через Superset SDK (модели Dashboard / Slice
# / SqlaTable). Идемпотентно: ищет дашборд по slug; если он уже существует —
# выходит без изменений (чтобы не затереть ручные правки в UI).
#
# Чарты в дашборде:
#   1. Bar chart    — средние очки за матч по командам (feat_team_form)
#   2. Line chart   — тренд формы топ-5 команд (feat_team_form, по match_date)
#   3. Table        — H2H последних встреч (feat_team_h2h + dim_team)
#   4. Big number   — общее количество отслеживаемых команд (dim_team)
#
# Запуск (внутри контейнера Superset):
#   python /app/pythonpath/dashboards/team_form_overview.py
# Либо через оркестратор:
#   python /app/pythonpath/dashboards/import_dashboards.py
# =============================================================================
from __future__ import annotations

import json
import logging
from typing import Any

# pylint: disable=import-error  # Superset imports resolved at runtime in container
from superset import db  # type: ignore
from superset.app import create_app  # type: ignore
from superset.connectors.sqla.models import SqlaTable  # type: ignore
from superset.models.core import Database  # type: ignore
from superset.models.dashboard import Dashboard  # type: ignore
from superset.models.slice import Slice  # type: ignore

log = logging.getLogger("dashboard.team_form_overview")

DASHBOARD_TITLE = "Team form overview"
DASHBOARD_SLUG = "team-form-overview"
DATABASE_NAME = "trino_iceberg"


def _find_table(database: Database, schema: str, table_name: str) -> SqlaTable:
    """Найти dataset по (database, schema, table). Бросает если не найдена."""
    table = (
        db.session.query(SqlaTable)
        .filter_by(database_id=database.id, schema=schema, table_name=table_name)
        .one_or_none()
    )
    if table is None:
        raise RuntimeError(
            f"dataset {schema}.{table_name} not found in database "
            f"'{database.database_name}'. Run import_datasources.py first."
        )
    return table


def _make_slice(
    name: str,
    viz_type: str,
    table: SqlaTable,
    params: dict[str, Any],
) -> Slice:
    """Создать Slice (если ещё нет с таким же slice_name) и вернуть."""
    existing = (
        db.session.query(Slice)
        .filter_by(slice_name=name, datasource_id=table.id, datasource_type="table")
        .one_or_none()
    )
    if existing is not None:
        log.info("slice '%s' already exists (id=%s); reusing", name, existing.id)
        return existing

    # Superset ожидает datasource ссылку строкой "<id>__table"
    full_params = dict(params)
    full_params.setdefault("datasource", f"{table.id}__table")
    full_params.setdefault("viz_type", viz_type)

    slc = Slice(
        slice_name=name,
        viz_type=viz_type,
        datasource_type="table",
        datasource_id=table.id,
        datasource_name=f"{table.schema}.{table.table_name}",
        params=json.dumps(full_params),
        query_context=None,
    )
    db.session.add(slc)
    db.session.commit()
    log.info("created slice '%s' (id=%s, viz=%s)", name, slc.id, viz_type)
    return slc


def _build_slices(database: Database) -> list[Slice]:
    """Построить 4 чарта дашборда."""
    feat_team_form = _find_table(database, "gold", "feat_team_form")
    feat_team_h2h = _find_table(database, "gold", "feat_team_h2h")
    dim_team = _find_table(database, "gold", "dim_team")

    slices: list[Slice] = []

    # ---- (1) Bar chart: avg points per match per team ----------------------
    bar_params: dict[str, Any] = {
        "metrics": [
            {
                "label": "AVG(points_per_match)",
                "expressionType": "SIMPLE",
                "aggregate": "AVG",
                "column": {"column_name": "points_per_match"},
            }
        ],
        "groupby": ["team_id"],
        "row_limit": 20,
        "order_desc": True,
        "show_legend": False,
        "y_axis_format": ".2f",
    }
    slices.append(
        _make_slice(
            "Avg points per match (by team)",
            "dist_bar",
            feat_team_form,
            bar_params,
        )
    )

    # ---- (2) Line chart: form trend, top-5 teams ---------------------------
    line_params: dict[str, Any] = {
        "x_axis": "match_date",
        "metrics": [
            {
                "label": "AVG(form_score)",
                "expressionType": "SIMPLE",
                "aggregate": "AVG",
                "column": {"column_name": "form_score"},
            }
        ],
        "groupby": ["team_name"],
        "time_grain_sqla": "P1W",
        "row_limit": 5000,
        "show_legend": True,
        "rich_tooltip": True,
    }
    slices.append(
        _make_slice(
            "Team form trend (top teams)",
            "echarts_timeseries_line",
            feat_team_form,
            line_params,
        )
    )

    # ---- (3) Table: head-to-head recent meetings ---------------------------
    table_params: dict[str, Any] = {
        "all_columns": [
            "match_id",
            "team_id",
            "opponent_id",
            "h2h_wins_l5",
            "h2h_draws_l5",
            "h2h_losses_l5",
            "h2h_goals_for_l5",
            "h2h_goals_against_l5",
        ],
        "row_limit": 50,
        "order_by_cols": ['["match_id", false]'],
        "table_timestamp_format": "smart_date",
    }
    slices.append(
        _make_slice(
            "Head-to-head — last 5 meetings",
            "table",
            feat_team_h2h,
            table_params,
        )
    )

    # ---- (4) Big number: total tracked teams -------------------------------
    bignum_params: dict[str, Any] = {
        "metric": {
            "label": "COUNT(DISTINCT team_id)",
            "expressionType": "SIMPLE",
            "aggregate": "COUNT_DISTINCT",
            "column": {"column_name": "team_id"},
        },
        "subheader": "Tracked teams",
        "y_axis_format": "SMART_NUMBER",
    }
    slices.append(
        _make_slice(
            "Tracked teams (total)",
            "big_number_total",
            dim_team,
            bignum_params,
        )
    )

    return slices


def create_dashboard() -> Dashboard | None:
    """Создать дашборд (если его ещё нет). Возвращает Dashboard или None."""
    app = create_app()
    with app.app_context():
        existing = (
            db.session.query(Dashboard).filter_by(slug=DASHBOARD_SLUG).one_or_none()
        )
        if existing is not None:
            log.info(
                "dashboard '%s' already exists (id=%s); skipping",
                DASHBOARD_SLUG,
                existing.id,
            )
            return existing

        database = (
            db.session.query(Database).filter_by(database_name=DATABASE_NAME).one_or_none()
        )
        if database is None:
            raise RuntimeError(
                f"database '{DATABASE_NAME}' not found. "
                "Run import_datasources.py first."
            )

        slices = _build_slices(database)

        dashboard = Dashboard(
            dashboard_title=DASHBOARD_TITLE,
            slug=DASHBOARD_SLUG,
            published=True,
            slices=slices,
        )
        db.session.add(dashboard)
        db.session.commit()
        log.info(
            "created dashboard '%s' (id=%s, slices=%d)",
            DASHBOARD_TITLE,
            dashboard.id,
            len(slices),
        )
        return dashboard


if __name__ == "__main__":
    logging.basicConfig(
        level="INFO",
        format="[dashboard] %(levelname)s %(message)s",
    )
    create_dashboard()
