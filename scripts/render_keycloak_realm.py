#!/usr/bin/env python3
"""Рендер realm-импорта Keycloak из шаблона (паттерн как у seaweedfs s3.config.json).

Читает configs/keycloak/realm-football.json.example, подставляет __VAR__ из .env,
пишет configs/keycloak/realm-football.json (в .gitignore, chmod 600).
Падает, если какой-то __VAR__ не найден в .env или остался в результате.
"""

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
TEMPLATE = REPO / "configs/keycloak/realm-football.json.example"
TARGET = REPO / "configs/keycloak/realm-football.json"
ENV_FILE = REPO / ".env"


def load_env(path: Path) -> dict[str, str]:
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip()
    return env


def main() -> int:
    if not ENV_FILE.exists():
        print(f"ERROR: {ENV_FILE} не найден", file=sys.stderr)
        return 1
    env = load_env(ENV_FILE)
    text = TEMPLATE.read_text()

    def substitute(match: re.Match) -> str:
        var = match.group(1)
        if var not in env or not env[var] or env[var].startswith("<"):
            raise KeyError(var)
        return env[var]

    try:
        rendered = re.sub(r"__([A-Z0-9_]+)__", substitute, text)
    except KeyError as e:
        print(f"ERROR: в .env нет значения для {e}", file=sys.stderr)
        return 1

    json.loads(rendered)  # валидность JSON после подстановки
    TARGET.write_text(rendered)
    # 644, не 600: контейнер keycloak работает от UID 1000 и должен читать файл;
    # на хосте каталог /root и так 700
    TARGET.chmod(0o644)
    print(f"OK: {TARGET.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
