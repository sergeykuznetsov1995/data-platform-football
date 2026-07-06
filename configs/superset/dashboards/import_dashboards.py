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

import logging
import subprocess
import sys
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
    "player_overview",
    "league_overview",
]


def main() -> int:
    here = Path(__file__).resolve().parent

    failures: list[tuple[str, str]] = []

    for module_name in DASHBOARDS:
        script = here / f"{module_name}.py"
        log.info("running dashboard module: %s", module_name)
        if not script.exists():
            log.error("module script not found: %s", script)
            failures.append((module_name, "script not found"))
            continue

        # Каждый модуль — в отдельном процессе: superset.app.create_app()
        # нельзя вызвать дважды в одном процессе (FAB/flask-limiter держат
        # глобальное состояние и падают на повторной регистрации views).
        proc = subprocess.run([sys.executable, str(script)], cwd=str(here))
        if proc.returncode != 0:
            log.error("module '%s' failed with exit code %d",
                      module_name, proc.returncode)
            failures.append((module_name, f"exit code {proc.returncode}"))

    if failures:
        log.error("dashboard import finished with %d failure(s):", len(failures))
        for name, reason in failures:
            log.error("  - %s: %s", name, reason)
        return 1

    log.info("all %d dashboard(s) imported successfully", len(DASHBOARDS))
    return 0


if __name__ == "__main__":
    sys.exit(main())
