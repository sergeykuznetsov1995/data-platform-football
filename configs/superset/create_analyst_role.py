#!/usr/bin/env python3
"""Роль analyst_data: доступ Gamma-аналитиков к базе trino_iceberg.

Идемпотентно: создаёт роль, если нет, и вешает на неё database_access
на trino_iceberg (это открывает её датасеты и SQL Lab). Вызывается из
bootstrap.sh или вручную:
  docker compose exec superset python /app/pythonpath/create_analyst_role.py
"""
import logging

from superset.app import create_app

log = logging.getLogger(__name__)

ROLE = "analyst_data"
DATABASE = "trino_iceberg"


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset import db, security_manager
        from superset.models.core import Database

        role = security_manager.find_role(ROLE) or security_manager.add_role(ROLE)

        database = (
            db.session.query(Database).filter_by(database_name=DATABASE).one_or_none()
        )
        if database is None:
            raise SystemExit(f"база {DATABASE} не найдена — сначала импорт datasources")

        perm = security_manager.find_permission_view_menu("database_access", database.perm)
        if perm is None:
            raise SystemExit(f"permission database_access {database.perm} не найден")
        if perm not in role.permissions:
            security_manager.add_permission_role(role, perm)
        db.session.commit()
        print(f"OK: роль {ROLE} с доступом к {DATABASE}")


if __name__ == "__main__":
    main()
