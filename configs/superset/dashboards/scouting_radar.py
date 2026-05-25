"""Scouting Radar dashboard.

Аудитория: скаут / аналитик. Бизнес-вопрос: «кто из игроков сейчас в форме
по xG/xA/defensive_actions, и кого стоит сравнить?». Источник —
iceberg.gold.mart_scouting_radar (per-(player, match) с L5 rolling).

Все slice spec'и — minimum viable: viz_type + metrics/groupby. Полный layout
JSON генерируется в T8.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from import_dashboards import Dashboard  # noqa: E402

DATASET = "mart_scouting_radar"

DASHBOARD = Dashboard(
    slug="scouting_radar",
    title="Scouting Radar — игроки в форме (L5)",
    description=(
        "Скаутинг: топ-N игроков по xG/xA/defensive_actions, тренды и "
        "сравнение по позициям. Данные обновляются раз в 2 часа."
    ),
    datasets=[DATASET],
    charts=[
        {
            "slice_name": "top10_xg_l5",
            "viz_type": "table",
            "dataset": DATASET,
            "params": {
                "metrics": ["xg_l5", "xa_l5", "shots_l5"],
                "groupby": ["player_name", "team_name", "position"],
                "row_limit": 10,
                "order_desc": True,
                "include_search": True,
            },
        },
        {
            "slice_name": "xg_vs_xa_scatter",
            "viz_type": "bubble_v2",
            "dataset": DATASET,
            "params": {
                "x_metric": "xg_l5",
                "y_metric": "xa_l5",
                "size_metric": "shots_l5",
                "entity": "player_name",
                "row_limit": 200,
            },
        },
        {
            "slice_name": "xg_l5_timeseries",
            "viz_type": "line",
            "dataset": DATASET,
            "params": {
                "metrics": ["xg_l5"],
                "groupby": ["player_name"],
                "x_axis": "match_date",
                "row_limit": 1000,
            },
        },
        {
            "slice_name": "defensive_actions_hist",
            "viz_type": "histogram",
            "dataset": DATASET,
            "params": {
                "all_columns_x": ["defensive_actions"],
                "bins": 20,
                "row_limit": 5000,
            },
        },
    ],
)
