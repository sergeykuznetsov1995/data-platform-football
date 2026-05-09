#!/usr/bin/env python
# =============================================================================
# Superset dashboards orchestrator
# =============================================================================
# Импортирует все declarative-дашборды из этой директории. Идемпотентно: каждый
# модуль проверяет slug перед созданием и пропускает уже существующий.
#
# Чтобы добавить новый дашборд:
#   1) Создать <name>.py с функцией create_dashboard() (см. README.md).
#   2) Добавить имя модуля в DASHBOARDS ниже.
#   3) Перезапустить bootstrap (`make superset-init`) или запустить вручную:
#        python /app/pythonpath/dashboards/import_dashboards.py
# =============================================================================
from __future__ import annotations

import importlib
import logging
import sys
import traceback
from pathlib import Path

logging.basicConfig(
    level="INFO",
    format="[dashboards-import] %(levelname)s %(message)s",
)
log = logging.getLogger("import_dashboards")


# -----------------------------------------------------------------------------
# Реестр дашбордов. Имя соответствует module name (без .py).
# -----------------------------------------------------------------------------
DASHBOARDS: list[str] = [
    "team_form_overview",
    "match_outcomes",
]


def main() -> int:
    # Гарантируем, что модули из текущей директории импортируются первыми.
    here = Path(__file__).resolve().parent
    if str(here) not in sys.path:
        sys.path.insert(0, str(here))

    failures: list[tuple[str, str]] = []

    for module_name in DASHBOARDS:
        log.info("importing dashboard module: %s", module_name)
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:  # pylint: disable=broad-except
            tb = traceback.format_exc()
            log.error("import failed for '%s': %s\n%s", module_name, exc, tb)
            failures.append((module_name, f"import: {exc}"))
            continue

        if not hasattr(module, "create_dashboard"):
            log.error("module '%s' does not define create_dashboard()", module_name)
            failures.append((module_name, "missing create_dashboard()"))
            continue

        try:
            module.create_dashboard()
        except Exception as exc:  # pylint: disable=broad-except
            tb = traceback.format_exc()
            log.error("create_dashboard() failed for '%s': %s\n%s", module_name, exc, tb)
            failures.append((module_name, f"create: {exc}"))

    if failures:
        log.error("dashboard import finished with %d failure(s):", len(failures))
        for name, reason in failures:
            log.error("  - %s: %s", name, reason)
        return 1

    log.info("all %d dashboard(s) imported successfully", len(DASHBOARDS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
