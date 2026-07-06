#!/bin/bash
# Онбординг юзера платформы одной командой (issue #861):
#   scripts/onboard_user.sh <username> <email> [группа]
# Группа: viewers (default — только Superset-дашборды) | analysts | platform-admins.
# Создаёт юзера в Keycloak (realm football), выдаёт временный пароль (смена при
# первом входе) и кладёт в группу. Правок конфигов и рестартов не нужно:
# права в Trino даёт catch-all в rules.json, роли в Superset — маппинг групп.
# Существующий username = громкая ошибка kcadm (скрипт не идемпотентен намеренно).
set -euo pipefail
cd "$(dirname "$0")/.."

USAGE="usage: onboard_user.sh <username> <email> [viewers|analysts|platform-admins]"
USERNAME=${1:?${USAGE}}
EMAIL=${2:?${USAGE}}
GROUP=${3:-viewers}
case "${GROUP}" in
    viewers|analysts|platform-admins) ;;
    *) echo "ERROR: неизвестная группа '${GROUP}'. ${USAGE}" >&2; exit 1 ;;
esac

KC_ADMIN=$(grep '^KC_BOOTSTRAP_ADMIN_USERNAME=' .env | cut -d= -f2-)
KC_ADMIN=${KC_ADMIN:-admin}
KC_PASS=$(grep '^KC_BOOTSTRAP_ADMIN_PASSWORD=' .env | cut -d= -f2-)
if [ -z "${KC_PASS}" ] || [[ "${KC_PASS}" == \<* ]]; then
    echo "ERROR: KC_BOOTSTRAP_ADMIN_PASSWORD не задан в .env" >&2
    exit 1
fi
TEMP_PASSWORD=$(openssl rand -base64 12)

kcadm() {
    docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh "$@"
}

kcadm config credentials --server http://localhost:8080 --realm master \
    --user "${KC_ADMIN}" --password "${KC_PASS}" >/dev/null

kcadm create users -r football \
    -s "username=${USERNAME}" -s "email=${EMAIL}" -s enabled=true

kcadm set-password -r football --username "${USERNAME}" \
    --new-password "${TEMP_PASSWORD}" --temporary

USER_ID=$(kcadm get users -r football -q "username=${USERNAME}" -q exact=true --fields id \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])')
GROUP_ID=$(kcadm get groups -r football --fields id,name \
    | GROUP="${GROUP}" python3 -c 'import json,os,sys; print(next(g["id"] for g in json.load(sys.stdin) if g["name"] == os.environ["GROUP"]))')
kcadm update "users/${USER_ID}/groups/${GROUP_ID}" -r football -n

echo "OK: юзер ${USERNAME} заведён в группе ${GROUP}"
echo "Временный пароль (Keycloak попросит сменить при первом входе): ${TEMP_PASSWORD}"
