#!/usr/bin/env python
# =============================================================================
# Superset dashboard: "Match outcomes"
# =============================================================================
# Декларативный дашборд по результатам матчей. Идемпотентен: проверяет slug
# и выходит, если уже создан.
#
# Чарты:
#   1. Pie chart    — распределение исходов home_win / draw / away_win (fct_match)
#   2. Heatmap      — голы home vs away (fct_match)
#   3. Time-series  — количество матчей по неделям (fct_match)
#   4. Top-10 bar   — команды с лучшим xG-перевесом (fct_team_match)
#
# Запуск:
#   python /app/pythonpath/dashboards/match_outcomes.py
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

log = logging.getLogger("dashboard.match_outcomes")

DASHBOARD_TITLE = "Match outcomes"
DASHBOARD_SLUG = "match-outcomes"
DATABASE_NAME = "trino_iceberg"


def _find_table(database: Database, schema: str, table_name: str) -> SqlaTable:
    table = (
        db.session.query(SqlaTable)
        .filter_by(database_id=database.id, schema=schema, table_name=table_name)
        .one_or_none()
    )
    if table is None:
        raise RuntimeError(
            f"dataset {schema}.{table_name} not found. "
            "Run import_datasources.py first."
        )
    return table


def _make_slice(
    name: str,
    viz_type: str,
    table: SqlaTable,
    params: dict[str, Any],
) -> Slice:
    existing = (
        db.session.query(Slice)
        .filter_by(slice_name=name, datasource_id=table.id, datasource_type="table")
        .one_or_none()
    )
    if existing is not None:
        log.info("slice '%s' already exists (id=%s); reusing", name, existing.id)
        return existing

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
    fct_match = _find_table(database, "gold", "fct_match")
    fct_team_match = _find_table(database, "gold", "fct_team_match")

    slices: list[Slice] = []

    # ---- (1) Pie chart: outcome distribution -------------------------------
    pie_params: dict[str, Any] = {
        "metric": {
            "label": "COUNT(*)",
            "expressionType": "SIMPLE",
            "aggregate": "COUNT",
            "column": {"column_name": "match_id"},
        },
        "groupby": ["result_1x2"],
        "row_limit": 10,
        "show_legend": True,
        "label_type": "key_percent",
    }
    slices.append(
        _make_slice(
            "Outcomes — home/draw/away",
            "pie",
            fct_match,
            pie_params,
        )
    )

    # ---- (2) Heatmap: home goals vs away goals -----------------------------
    heatmap_params: dict[str, Any] = {
        "all_columns_x": "home_goals",
        "all_columns_y": "away_goals",
        "metric": {
            "label": "COUNT(*)",
            "expressionType": "SIMPLE",
            "aggregate": "COUNT",
            "column": {"column_name": "match_id"},
        },
        "row_limit": 1000,
        "linear_color_scheme": "blue_white_yellow",
        "show_legend": True,
        "show_values": True,
    }
    slices.append(
        _make_slice(
            "Goals heatmap (home vs away)",
            "heatmap",
            fct_match,
            heatmap_params,
        )
    )

    # ---- (3) Time-series: matches per week ---------------------------------
    weekly_params: dict[str, Any] = {
        "x_axis": "match_date",
        "metrics": [
            {
                "label": "COUNT(*)",
                "expressionType": "SIMPLE",
                "aggregate": "COUNT",
                "column": {"column_name": "match_id"},
            }
        ],
        "time_grain_sqla": "P1W",
        "row_limit": 1000,
        "show_legend": False,
        "rich_tooltip": True,
    }
    slices.append(
        _make_slice(
            "Matches per week",
            "echarts_timeseries_line",
            fct_match,
            weekly_params,
        )
    )

    # ---- (4) Top-10 bar: teams with best xG advantage ----------------------
    top_xg_params: dict[str, Any] = {
        "metrics": [
            {
                "label": "SUM(xg_diff)",
                "expressionType": "SIMPLE",
                "aggregate": "SUM",
                "column": {"column_name": "xg_diff"},
            }
        ],
        "groupby": ["team_id"],
        "row_limit": 10,
        "order_desc": True,
        "show_legend": False,
        "y_axis_format": ".2f",
    }
    slices.append(
        _make_slice(
            "Top-10 teams by xG advantage",
            "dist_bar",
            fct_team_match,
            top_xg_params,
        )
    )

    return slices


def create_dashboard() -> Dashboard | None:
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
