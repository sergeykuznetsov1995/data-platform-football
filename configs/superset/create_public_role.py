#!/usr/bin/env python3
"""Роль Public: гостевой просмотр дашбордов без логина (#1570).

Идемпотентно: приводит права роли Public РОВНО к набору ниже (лишние
снимает), чтобы повторный запуск всегда давал одно и то же состояние;
если чего-то не хватает (право, дашборд) — завершается ошибкой.
Гость получает только просмотр дашбордов из DASHBOARD_SLUGS и их датасетов:
без Explore, SQL Lab, экспорта и любых записей. PUBLIC_ROLE_LIKE в
superset_config.py намеренно не задан — иначе `superset init` подмешал бы
права Gamma (сохранение чартов/дашбордов). Вызывается из bootstrap.sh после
импорта дашбордов или вручную:
  docker compose exec superset python /app/pythonpath/create_public_role.py
"""
from __future__ import annotations

import logging
from typing import Any

log = logging.getLogger(__name__)

ROLE = "Public"

DASHBOARD_SLUGS = (
    "league-overview",
    "player-overview-league",
    "world-cup",
)

# (permission, view_menu) — имена как в ab_permission_view Superset 4.1.
VIEW_PERMISSIONS = (
    ("can_dashboard", "Superset"),            # /superset/dashboard/<slug>/
    ("can_dashboard_permalink", "Superset"),  # /superset/dashboard/p/<key>/
    ("can_read", "Dashboard"),
    ("can_read", "Chart"),                    # /api/v1/chart/data — данные чартов и фильтров
    ("can_read", "Dataset"),                  # колонки датасета для нативных фильтров
    ("can_read", "CssTemplate"),
    ("can_read", "DashboardFilterStateRestApi"),
    ("can_write", "DashboardFilterStateRestApi"),  # состояние фильтров в кэше
    ("can_read", "DashboardPermalinkRestApi"),
    ("can_read", "SecurityRestApi"),          # csrf-токен для POST filter_state
    ("can_get", "MenuApi"),                   # меню шапки
    ("can_time_range", "Api"),                # подписи временных фильтров
    ("menu_access", "Dashboards"),
)


def run(security_manager: Any, session: Any, dashboard_model: Any) -> dict[str, list[str]]:
    """Собрать нужные права и привести к ним роль. Возвращает добавленные/снятые.

    Отсутствующее право или дашборд — ошибка (SystemExit): роль всё равно
    приводится к тому, что нашлось, но bootstrap/деплой не должны выглядеть
    успешными с неполным набором.
    """
    role = security_manager.find_role(ROLE) or security_manager.add_role(ROLE)

    wanted, missing = [], []
    for permission, view_menu in VIEW_PERMISSIONS:
        pvm = security_manager.find_permission_view_menu(permission, view_menu)
        if pvm is None:
            missing.append(f"permission {permission} on {view_menu}")
            continue
        wanted.append(pvm)

    for slug in DASHBOARD_SLUGS:
        dashboard = session.query(dashboard_model).filter_by(slug=slug).one_or_none()
        if dashboard is None:
            missing.append(f"dashboard {slug} (сначала импорт дашбордов)")
            continue
        for datasource in dashboard.datasources:
            pvm = security_manager.find_permission_view_menu(
                "datasource_access", datasource.perm
            ) or security_manager.add_permission_view_menu(
                "datasource_access", datasource.perm
            )
            wanted.append(pvm)

    added, removed = [], []
    for pvm in list(role.permissions):
        if pvm not in wanted:
            security_manager.del_permission_role(role, pvm)
            removed.append(str(pvm))
    for pvm in wanted:
        if pvm not in role.permissions:
            security_manager.add_permission_role(role, pvm)
            added.append(str(pvm))
    session.commit()
    if missing:
        raise SystemExit("роль Public неполная, не найдено: " + "; ".join(missing))
    return {"added": added, "removed": removed}


def main() -> None:
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db, security_manager
        from superset.models.dashboard import Dashboard

        result = run(security_manager, db.session, Dashboard)
        print(
            f"OK: роль {ROLE}: +{len(result['added'])} прав, "
            f"-{len(result['removed'])} прав; дашборды: {', '.join(DASHBOARD_SLUGS)}"
        )


if __name__ == "__main__":
    main()
