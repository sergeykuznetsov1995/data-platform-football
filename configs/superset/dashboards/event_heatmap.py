"""Event Heatmap dashboard.

Аудитория: тактический штаб / аналитик соперника. Бизнес-вопрос:
«где на поле команда проявляет активность и какие действия успешны?».
Источник — iceberg.gold.mart_event_heatmap (12×8 grid поверх SPADL x/y).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from import_dashboards import Dashboard  # noqa: E402

DATASET = "mart_event_heatmap"

DASHBOARD = Dashboard(
    slug="event_heatmap",
    title="Event Heatmap — зоны активности (12×8)",
    description=(
        "Heatmap событий и успешности по 96 зонам поля (12×8). "
        "Фильтры по команде, сезону, типу действия (SPADL action_canonical)."
    ),
    datasets=[DATASET],
    charts=[
        {
            "slice_name": "event_count_heatmap",
            "viz_type": "heatmap",
            "dataset": DATASET,
            "params": {
                "all_columns_x": "zone_x",
                "all_columns_y": "zone_y",
                "metric": "event_count",
                "linear_color_scheme": "blue_white_yellow",
                "row_limit": 5000,
            },
        },
        {
            "slice_name": "success_rate_heatmap",
            "viz_type": "heatmap",
            "dataset": DATASET,
            "params": {
                "all_columns_x": "zone_x",
                "all_columns_y": "zone_y",
                "metric": "success_rate",
                "linear_color_scheme": "white_red_yellow",
                "y_axis_format": ".0%",
                "row_limit": 5000,
            },
        },
        {
            "slice_name": "action_canonical_bar",
            "viz_type": "dist_bar",
            "dataset": DATASET,
            "params": {
                "metrics": ["event_count"],
                "groupby": ["action_canonical"],
                "row_limit": 24,
                "order_desc": True,
                "show_bar_value": True,
                "x_ticks_layout": "45°",
            },
        },
        {
            "slice_name": "top_zones_table",
            "viz_type": "table",
            "dataset": DATASET,
            "params": {
                "metrics": ["event_count", "success_rate"],
                "groupby": ["team_name", "zone_x", "zone_y", "action_canonical"],
                "row_limit": 100,
                "order_desc": True,
                "include_search": True,
            },
        },
    ],
)
