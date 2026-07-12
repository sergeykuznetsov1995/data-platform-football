#!/usr/bin/env bash
# Генерация RSA-keypair для подписи бот-JWT OpenMetadata (#866).
# Дефолтные ключи образа openmetadata/server публично известны
# (kid Gb389a-9f76-gdjs-a92j-0242bk94356, приватник лежит в каждом образе) —
# с ними любой может подделать токен ingestion-bot и получить админ-доступ
# к API. Перед публикацией meta наружу свои ключи ОБЯЗАТЕЛЬНЫ.
# Формат — DER (PKCS#8 приватный + X.509 публичный), как ждёт
# jwtTokenConfiguration OM. После генерации прописать в .env:
#   OM_RSA_PUBLIC_KEY_FILE_PATH / OM_RSA_PRIVATE_KEY_FILE_PATH /
#   OM_JWT_ISSUER / OM_JWT_KEY_ID (см. .env.example) — и перевыпустить
#   OM_JWT_TOKEN (старые бот-токены после смены ключей невалидны).
set -euo pipefail

DIR="$(cd "$(dirname "$0")/.." && pwd)/configs/openmetadata/jwtkeys"

if [[ -f "${DIR}/private_key.der" ]]; then
    echo "ERROR: ${DIR}/private_key.der уже существует." >&2
    echo "Ротация инвалидирует ВСЕ бот-токены (OM_JWT_TOKEN придётся перевыпускать)." >&2
    echo "Если это осознанно — удалите каталог вручную и запустите снова." >&2
    exit 1
fi

mkdir -p "${DIR}"
TMP_PEM="${DIR}/.private_key.pem"
trap 'rm -f "${TMP_PEM}"' EXIT

openssl genrsa -out "${TMP_PEM}" 2048 2>/dev/null
openssl pkcs8 -topk8 -inform PEM -outform DER -in "${TMP_PEM}" \
    -out "${DIR}/private_key.der" -nocrypt
openssl rsa -in "${TMP_PEM}" -pubout -outform DER \
    -out "${DIR}/public_key.der" 2>/dev/null

# 644, не 600: контейнер OM работает от непривилегированного uid и должен
# читать файлы (тот же компромисс, что у render_keycloak_realm.py).
chmod 755 "${DIR}"
chmod 644 "${DIR}"/*.der

echo "OK: ключи в ${DIR}"
echo "Предлагаемый OM_JWT_KEY_ID=$(cat /proc/sys/kernel/random/uuid)"
echo "Дальше: раскомментировать OM_RSA_*/OM_JWT_ISSUER/OM_JWT_KEY_ID в .env"
echo "и перевыпустить токен ingestion-bot (Settings → Bots) в OM_JWT_TOKEN."
