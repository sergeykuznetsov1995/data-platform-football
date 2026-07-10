#!/bin/bash
# Включение саморегистрации на ЖИВОМ realm football (--import-realm пропускает
# существующий realm, поэтому правки configs/keycloak/realm-football.json.example
# на прод не действуют — их применяет этот скрипт). Идемпотентен.
# Делает: registrationAllowed=true, CSP для iframe Google, reCAPTCHA v2 (REQUIRED)
# в registration-flow (ключи из .env), дефолтная группа viewers для новых юзеров.
set -euo pipefail
cd "$(dirname "$0")/.."

KC_ADMIN=$(grep '^KC_BOOTSTRAP_ADMIN_USERNAME=' .env | cut -d= -f2-); KC_ADMIN=${KC_ADMIN:-admin}
KC_PASS=$(grep '^KC_BOOTSTRAP_ADMIN_PASSWORD=' .env | cut -d= -f2-)
SITE_KEY=$(grep '^KC_RECAPTCHA_SITE_KEY=' .env | cut -d= -f2-)
SECRET_KEY=$(grep '^KC_RECAPTCHA_SECRET_KEY=' .env | cut -d= -f2-)
for V in KC_PASS SITE_KEY SECRET_KEY; do
    if [ -z "${!V}" ] || [[ "${!V}" == \<* ]]; then
        echo "ERROR: не заполнен ${V} в .env (KC_BOOTSTRAP_ADMIN_PASSWORD / KC_RECAPTCHA_*)" >&2
        exit 1
    fi
done

kcadm() { docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh "$@"; }

kcadm config credentials --server http://localhost:8080 --realm master \
    --user "${KC_ADMIN}" --password "${KC_PASS}" >/dev/null

echo "== 1/3 Realm: registrationAllowed + CSP (frame-src для reCAPTCHA)"
kcadm update realms/football -s registrationAllowed=true \
    -s "browserSecurityHeaders.contentSecurityPolicy=frame-src 'self' https://www.google.com; frame-ancestors 'self'; object-src 'none';"

echo "== 2/3 reCAPTCHA в registration-flow: REQUIRED + ключи"
EXEC_JSON=$(kcadm get authentication/flows/registration/executions -r football)
# requirement меняем в ПОЛНОМ объекте execution: минимальное тело сбросило бы priority
echo "${EXEC_JSON}" | python3 -c '
import json, sys
e = next(x for x in json.load(sys.stdin) if x["providerId"] == "registration-recaptcha-action")
e["requirement"] = "REQUIRED"
print(json.dumps(e))' \
    | kcadm update authentication/flows/registration/executions -r football -f -
EXEC_ID=$(echo "${EXEC_JSON}" | python3 -c \
    'import json,sys; print(next(x["id"] for x in json.load(sys.stdin) if x["providerId"] == "registration-recaptcha-action"))')
CFG_ID=$(echo "${EXEC_JSON}" | python3 -c \
    'import json,sys; print(next(x.get("authenticationConfig","") for x in json.load(sys.stdin) if x["providerId"] == "registration-recaptcha-action"))')
if [ -z "${CFG_ID}" ]; then
    kcadm create "authentication/executions/${EXEC_ID}/config" -r football \
        -b "{\"alias\":\"recaptcha\",\"config\":{\"site.key\":\"${SITE_KEY}\",\"secret.key\":\"${SECRET_KEY}\",\"useRecaptchaNet\":\"false\",\"recaptcha.v3\":\"false\",\"action\":\"register\"}}"
else
    kcadm update "authentication/config/${CFG_ID}" -r football \
        -s "config.\"site.key\"=${SITE_KEY}" -s "config.\"secret.key\"=${SECRET_KEY}"
fi

echo "== 3/3 Дефолтная группа viewers для самозарегавшихся"
GROUP_ID=$(kcadm get groups -r football --fields id,name \
    | python3 -c 'import json,sys; print(next(g["id"] for g in json.load(sys.stdin) if g["name"] == "viewers"))')
# -n обязателен: merge-режим делает GET по этому URI, который отвечает 405
kcadm update "realms/football/default-groups/${GROUP_ID}" -n

echo "OK: саморегистрация включена (reCAPTCHA REQUIRED, новые юзеры -> viewers)"
