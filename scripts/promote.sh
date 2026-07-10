#!/bin/bash
# Полуавтоматический перевод самозарегавшихся в analysts:
#   scripts/promote.sh --list       — необработанные заявки «Прошу доступ к данным»
#   scripts/promote.sh <username>   — viewers -> analysts + пометить заявку
# Заявки пишет Superset (/access-request на bi.<домен>) в
# /app/superset_home/access_requests.txt (volume superset_home), строка =
# "username<TAB>дата"; обработанные получают третье поле "done".
# Права в Superset обновятся при следующем логине юзера (AUTH_ROLES_SYNC_AT_LOGIN).
set -euo pipefail
cd "$(dirname "$0")/.."

USAGE="usage: promote.sh --list | promote.sh <username>"
ARG=${1:?${USAGE}}
REQUESTS_FILE=/app/superset_home/access_requests.txt

if [ "${ARG}" = "--list" ]; then
    # необработанные = строки из двух полей (username, дата)
    PENDING=$(docker compose exec -T superset awk -F'\t' 'NF < 3' "${REQUESTS_FILE}" 2>/dev/null || true)
    if [ -z "${PENDING}" ]; then echo "заявок нет"; else echo "${PENDING}"; fi
    exit 0
fi
USERNAME=${ARG}

KC_ADMIN=$(grep '^KC_BOOTSTRAP_ADMIN_USERNAME=' .env | cut -d= -f2-); KC_ADMIN=${KC_ADMIN:-admin}
KC_PASS=$(grep '^KC_BOOTSTRAP_ADMIN_PASSWORD=' .env | cut -d= -f2-)
if [ -z "${KC_PASS}" ] || [[ "${KC_PASS}" == \<* ]]; then
    echo "ERROR: KC_BOOTSTRAP_ADMIN_PASSWORD не задан в .env" >&2
    exit 1
fi

kcadm() { docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh "$@"; }

kcadm config credentials --server http://localhost:8080 --realm master \
    --user "${KC_ADMIN}" --password "${KC_PASS}" >/dev/null

USER_ID=$(kcadm get users -r football -q "username=${USERNAME}" -q exact=true --fields id \
    | USERNAME="${USERNAME}" python3 -c 'import json,os,sys
users = json.load(sys.stdin)
if not users:
    sys.exit("ERROR: юзер %s не найден в Keycloak" % os.environ["USERNAME"])
print(users[0]["id"])')
GROUPS_JSON=$(kcadm get groups -r football --fields id,name)
ANALYSTS_ID=$(echo "${GROUPS_JSON}" | python3 -c \
    'import json,sys; print(next(g["id"] for g in json.load(sys.stdin) if g["name"] == "analysts"))')
VIEWERS_ID=$(echo "${GROUPS_JSON}" | python3 -c \
    'import json,sys; print(next(g["id"] for g in json.load(sys.stdin) if g["name"] == "viewers"))')

kcadm update "users/${USER_ID}/groups/${ANALYSTS_ID}" -r football -n
kcadm delete "users/${USER_ID}/groups/${VIEWERS_ID}" -r football

# пометить заявки юзера обработанными (файла может не быть — promote без заявки)
docker compose exec -T superset \
    sed -i "/^${USERNAME}\t[^\t]*\$/ s/\$/\tdone/" "${REQUESTS_FILE}" 2>/dev/null || true

echo "OK: ${USERNAME} переведён viewers -> analysts (Superset подхватит при следующем логине)"
