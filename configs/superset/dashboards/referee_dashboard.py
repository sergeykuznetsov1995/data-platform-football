"""Referee Dashboard.

Аудитория: букмекерские аналитики / тактический штаб. Бизнес-вопрос:
«как судья влияет на исход (карточки/пенальти/home bias)?». Источник —
iceberg.gold.mart_referee_dashboard (per-(referee, season, league)).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from import_dashboards import Dashboard  # noqa: E402

DATASET = "mart_referee_dashboard"

DASHBOARD = Dashboard(
    slug="referee_dashboard",
    title="Referee Dashboard — карточки и bias судей",
    description=(
        "Профиль судей: карточки на матч, штрафные удары, доля побед "
        "хозяев. Фильтры по сезону и лиге."
    ),
    datasets=[DATASET],
    charts=[
        {
            "slice_name": "cards_per_match_bar",
            "viz_type": "dist_bar",
            "dataset": DATASET,
            "params": {
                "metrics": ["cards_per_match"],
                "groupby": ["referee_name"],
                "row_limit": 25,
                "order_desc": True,
                "show_bar_value": True,
                "x_ticks_layout": "45°",
            },
        },
        {
            "slice_name": "cards_vs_goals_scatter",
            "viz_type": "bubble_v2",
            "dataset": DATASET,
            "params": {
                "x_metric": "cards_per_match",
                "y_metric": "goals_per_match",
                "size_metric": "matches_officiated",
                "entity": "referee_name",
                "row_limit": 200,
            },
        },
        {
            "slice_name": "home_win_pct_bar",
            "viz_type": "dist_bar",
            "dataset": DATASET,
            "params": {
                "metrics": ["home_win_pct"],
                "groupby": ["referee_name"],
                "row_limit": 25,
                "order_desc": True,
                "show_bar_value": True,
                "y_axis_format": ".1%",
                "x_ticks_layout": "45°",
            },
        },
        {
            "slice_name": "referees_full_table",
            "viz_type": "table",
            "dataset": DATASET,
            "params": {
                "metrics": [
                    "matches_officiated",
                    "cards_per_match",
                    "yellows_per_match",
                    "reds_per_match",
                    "penalties_per_match",
                    "home_win_pct",
                ],
                "groupby": ["referee_name", "season", "league"],
                "row_limit": 500,
                "include_search": True,
            },
        },
    ],
)
