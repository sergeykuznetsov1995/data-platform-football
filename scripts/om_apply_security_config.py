#!/usr/bin/env python3
"""Применить security-конфиг OpenMetadata (auth-провайдер) на ЖИВОМ сервере (#866).

Зачем отдельный скрипт: OM 1.13 читает `authenticationConfiguration` /
`authorizerConfiguration` **из своей БД**, а не из env — env использутся только
при первом старте (пустая таблица `openmetadata_settings`). На живом сервере
`docker compose up -d` с новыми `AUTHENTICATION_*` НИЧЕГО не переключит:
в логе будет `Loaded security configuration from database - provider: basic`.
Штатный путь (то, что делает UI Settings → Security) — `PUT /api/v1/system/
security/config`; он применяется на лету, без рестарта.

Скрипт: basic-логин админом → GET текущего конфига → патч значениями из env
(тех же, что в compose) → POST validate → PUT config. Идемпотентен.

Требует (обычно из .env, см. .env.example):
  OM_AUTH_PROVIDER, OM_AUTH_CLIENT_TYPE, OM_AUTH_PROVIDER_NAME, OM_AUTH_AUTHORITY,
  OM_AUTH_CLIENT_ID, OM_AUTH_CALLBACK_URL, OM_AUTH_PUBLIC_KEYS,
  OM_AUTH_ENABLE_SELF_SIGNUP, OM_AUTH_PRINCIPAL_CLAIMS, OM_ADMIN_PRINCIPALS,
  OPENMETADATA_OIDC_CLIENT_SECRET, OM_OIDC_DISCOVERY_URI, OM_PUBLIC_URL
плюс креды локального админа OM (basic-логин ещё доступен ДО переключения):
  OM_ADMIN_EMAIL (default admin@open-metadata.org), OM_ADMIN_PASSWORD

Использование:
  python3 scripts/om_apply_security_config.py --dry-run   # показать, что изменится
  python3 scripts/om_apply_security_config.py             # применить

ОТКАТ (break-glass) идёт НЕ через этот скрипт: в OIDC-режиме basic-логин
запрещён (403), а Keycloak может лежать. Откат — чистка БД + рестарт,
env-дефолты compose вернут basic:

  docker compose exec -T postgres psql -U openmetadata -d openmetadata -c \\
    "DELETE FROM openmetadata_settings WHERE configtype IN
     ('authenticationConfiguration','authorizerConfiguration');"
  docker compose --profile heavy up -d --force-recreate openmetadata-server
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

ENV_FILE = Path(__file__).resolve().parents[1] / ".env"

# Секции, которых нет в нашем сетапе: сервер отдаёт их пустыми болванками,
# а валидация PUT требует в них обязательные поля (host/port/...) — шлём без них.
DROP_SECTIONS = ("ldapConfiguration", "samlConfiguration")


def load_env() -> None:
    """Подмешать .env в окружение (реальное окружение имеет приоритет).

    Читаем файл сами, а не через `set -a; . ./.env`: в .env есть значения-заглушки
    вида `<your-token>`, на которых shell-source падает (`<` = редирект).
    """
    if not ENV_FILE.exists():
        return
    for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_env()

OM_URL = os.environ.get("OM_BASE_URL", "http://127.0.0.1:8585")
# Email локального админа = admin@<AUTHORIZER_PRINCIPAL_DOMAIN>. У нас в compose
# домен `openmetadata.org`, дефолт образа — `open-metadata.org`: пробуем оба.
ADMIN_EMAILS = (
    [os.environ["OM_ADMIN_EMAIL"]]
    if os.environ.get("OM_ADMIN_EMAIL")
    else ["admin@openmetadata.org", "admin@open-metadata.org"]
)


def _req(method: str, path: str, token: str | None = None, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(f"{OM_URL}{path}", data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, {"message": raw.decode(errors="replace")}


def _env_list(name: str) -> list[str] | None:
    """OM-style список `[a, b]` из .env → python-список."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    return [item.strip() for item in raw.strip("[]").split(",") if item.strip()]


def login() -> str:
    password = os.environ.get("OM_ADMIN_PASSWORD")
    if not password:
        sys.exit("OM_ADMIN_PASSWORD не задан (локальный админ OM, basic-логин)")
    encoded = base64.b64encode(password.encode()).decode()
    last = None
    for email in ADMIN_EMAILS:
        status, body = _req(
            "POST", "/api/v1/users/login", body={"email": email, "password": encoded}
        )
        if status == 403:
            sys.exit(
                "basic-логин запрещён — сервер уже в SSO-режиме. Повторное применение "
                "конфига делайте из UI (Settings → Security); откат — см. docstring."
            )
        if status == 200 and "accessToken" in body:
            return body["accessToken"]
        last = (email, status, body)
    sys.exit(f"логин админа не прошёл ни для одного email: {last}")


def build_config(current: dict) -> dict:
    cfg = json.loads(json.dumps(current))  # deep copy
    auth = cfg["authenticationConfiguration"]
    for section in DROP_SECTIONS:
        auth.pop(section, None)

    provider = os.environ.get("OM_AUTH_PROVIDER")
    if not provider:
        sys.exit("OM_AUTH_PROVIDER не задан — нечего применять")

    auth["provider"] = provider
    auth["providerName"] = os.environ.get("OM_AUTH_PROVIDER_NAME", "")
    auth["clientType"] = os.environ.get("OM_AUTH_CLIENT_TYPE", "public")
    auth["authority"] = os.environ.get("OM_AUTH_AUTHORITY", auth.get("authority", ""))
    auth["clientId"] = os.environ.get("OM_AUTH_CLIENT_ID", "")
    auth["callbackUrl"] = os.environ.get("OM_AUTH_CALLBACK_URL", "")

    public_keys = _env_list("OM_AUTH_PUBLIC_KEYS")
    if public_keys:
        # Собственный JWKS OM обязан остаться в списке: на нём валидируются
        # бот-токены ingestion-контура (см. compose.yaml).
        if not any("system/config/jwks" in url for url in public_keys):
            sys.exit(
                "OM_AUTH_PUBLIC_KEYS без собственного JWKS OM "
                "(…/api/v1/system/config/jwks) — бот-JWT перестанут валидироваться"
            )
        auth["publicKeyUrls"] = public_keys

    claims = _env_list("OM_AUTH_PRINCIPAL_CLAIMS")
    if claims:
        auth["jwtPrincipalClaims"] = claims

    self_signup = os.environ.get("OM_AUTH_ENABLE_SELF_SIGNUP")
    if self_signup is not None:
        auth["enableSelfSignup"] = self_signup.lower() == "true"

    oidc = auth.get("oidcConfiguration") or {}
    oidc.update(
        {
            "id": os.environ.get("OM_AUTH_CLIENT_ID", ""),
            "type": os.environ.get("OM_AUTH_PROVIDER_NAME", ""),
            "secret": os.environ.get("OPENMETADATA_OIDC_CLIENT_SECRET", ""),
            "discoveryUri": os.environ.get("OM_OIDC_DISCOVERY_URI", ""),
            "callbackUrl": os.environ.get("OM_AUTH_CALLBACK_URL", ""),
            "serverUrl": os.environ.get("OM_PUBLIC_URL", ""),
        }
    )
    auth["oidcConfiguration"] = oidc

    admins = _env_list("OM_ADMIN_PRINCIPALS")
    if admins:
        cfg["authorizerConfiguration"]["adminPrincipals"] = admins

    return cfg


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="только показать конфиг")
    args = parser.parse_args()

    token = login()
    status, current = _req("GET", "/api/v1/system/security/config", token)
    if status != 200:
        sys.exit(f"не удалось прочитать текущий конфиг: {status} {current}")

    print(f"сейчас: provider={current['authenticationConfiguration']['provider']}")
    new_cfg = build_config(current)
    auth = new_cfg["authenticationConfiguration"]
    print(
        f"станет: provider={auth['provider']} clientType={auth['clientType']} "
        f"selfSignup={auth['enableSelfSignup']} "
        f"admins={new_cfg['authorizerConfiguration']['adminPrincipals']}"
    )

    if args.dry_run:
        print(json.dumps(new_cfg, indent=2, ensure_ascii=False))
        return

    status, body = _req("POST", "/api/v1/system/security/validate", token, new_cfg)
    if status != 200:
        sys.exit(f"валидация конфига не прошла: {status} {body}")

    status, body = _req("PUT", "/api/v1/system/security/config", token, new_cfg)
    if status != 200:
        sys.exit(f"применение конфига не прошло: {status} {body}")

    status, live = _req("GET", "/api/v1/system/config/auth")
    print(
        f"OK: provider={live.get('provider')} clientType={live.get('clientType')} "
        f"selfSignup={live.get('enableSelfSignup')}"
    )
    print("Проверь вход через браузер; бот-JWT продолжает работать (ключи не менялись).")


if __name__ == "__main__":
    main()
