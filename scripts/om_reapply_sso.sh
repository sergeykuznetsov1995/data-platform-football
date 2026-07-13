#!/usr/bin/env bash
# Применить SSO-конфиг OpenMetadata, когда basic-логин уже закрыт (#866).
#
# Когда нужен: конфиг OM живёт в БД, и если его снести (откат/пересборка),
# сервер восстановит его из env — но БЕЗ секрета OIDC-клиента и с дефолтным
# responseType. Вход тогда падает на обмене кода: в логе
#   TechnicalException: Bad token response, error=unauthorized_client
# Чинится повторным применением конфига через API — но basic-логин в SSO-режиме
# отвечает 403, поэтому авторизуемся Bearer'ом админа-человека из Keycloak.
#
# Как получаем Bearer: временно включаем password-grant на клиенте openmetadata,
# берём токен, сразу выключаем обратно (trap на выход).
#
# Запуск (пароль вводится скрыто, в историю команд не попадает):
#   KC_USER=<логин> bash scripts/om_reapply_sso.sh
set -euo pipefail

cd "$(dirname "$0")/.."

: "${KC_USER:?укажи KC_USER=<логин админа в Keycloak, из OM_ADMIN_PRINCIPALS>}"
if [[ -z "${KC_PASSWORD:-}" ]]; then
    read -rs -p "Пароль ${KC_USER} в Keycloak: " KC_PASSWORD
    echo
fi

env_get() { grep -m1 "^$1=" .env | cut -d= -f2- | tr -d '\r'; }
KC_ADMIN=$(env_get KC_BOOTSTRAP_ADMIN_USERNAME)
KC_ADMIN_PASS=$(env_get KC_BOOTSTRAP_ADMIN_PASSWORD)
OM_SECRET=$(env_get OPENMETADATA_OIDC_CLIENT_SECRET)
ISSUER=$(env_get OM_AUTH_AUTHORITY)
CLIENT=$(env_get OM_AUTH_CLIENT_ID)

kc() { docker exec -i keycloak /opt/keycloak/bin/kcadm.sh "$@"; }

kc config credentials --server http://localhost:8080 --realm master \
    --user "$KC_ADMIN" --password "$KC_ADMIN_PASS" >/dev/null
CID=$(kc get clients -r football -q "clientId=${CLIENT}" --fields id --format csv --noquotes | tr -d '\r' | head -1)
[[ -n "$CID" ]] || { echo "клиент ${CLIENT} не найден в Keycloak" >&2; exit 1; }

restore() { kc update "clients/$CID" -r football -s directAccessGrantsEnabled=false >/dev/null 2>&1 || true; }
trap restore EXIT

echo "1/3 временно включаю password-grant на клиенте ${CLIENT}"
kc update "clients/$CID" -r football -s directAccessGrantsEnabled=true

echo "2/3 получаю токен ${KC_USER}"
TOKEN=$(curl -s -X POST "${ISSUER}/protocol/openid-connect/token" \
    -d grant_type=password -d "client_id=${CLIENT}" \
    --data-urlencode "client_secret=${OM_SECRET}" \
    --data-urlencode "username=${KC_USER}" --data-urlencode "password=${KC_PASSWORD}" \
    -d scope=openid \
    | python3 -c 'import sys,json; print(json.load(sys.stdin).get("access_token") or "")')
[[ -n "$TOKEN" ]] || { echo "не удалось получить токен — проверь логин/пароль" >&2; exit 1; }

echo "3/3 применяю конфиг из .env через API OpenMetadata"
OM_BEARER="$TOKEN" python3 scripts/om_apply_security_config.py "$@"
