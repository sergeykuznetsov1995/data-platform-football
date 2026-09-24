#!/usr/bin/env bash
# Заморозка дерева контура Transfermarkt под ротацию (#1387; образец deploy/sofascore).
# Использование: bash deploy/transfermarkt/freeze_release.sh <sha>
# Переменные — из $TRANSFERMARKT_ENV_FILE (по умолчанию /etc/data-platform/transfermarkt.env):
#   TRANSFERMARKT_SOURCE_REPO, TRANSFERMARKT_RELEASES_DIR.
# Дальше: deploy.sh <дерево>.
set -euo pipefail

# Замороженное дерево монтируется read-only в контейнеры под uid 50000. Шелл агента может
# унаследовать umask 0077 — тогда git создаст 0700/0600, которые контур не прочитает.
umask 0022

SHA="${1:?sha коммита для заморозки}"
ENV_FILE="${TRANSFERMARKT_ENV_FILE:-/etc/data-platform/transfermarkt.env}"
# shellcheck source=deploy/transfermarkt/env.sh
. "$(dirname "$0")/env.sh"
transfermarkt_load_env "$ENV_FILE" || exit 2
: "${TRANSFERMARKT_SOURCE_REPO:?}" "${TRANSFERMARKT_RELEASES_DIR:?}"

[ -d "$TRANSFERMARKT_RELEASES_DIR" ] || { echo "нет каталога релизов $TRANSFERMARKT_RELEASES_DIR" >&2; exit 1; }
TMP_TREE=$(mktemp -d "$TRANSFERMARKT_RELEASES_DIR/freeze.XXXXXX")
trap 'rm -rf "$TMP_TREE"' EXIT

# Без --shared: дерево живёт неделями и не должно зависеть от gc в исходном репо.
git clone -q "$TRANSFERMARKT_SOURCE_REPO" "$TMP_TREE"
git -C "$TMP_TREE" checkout -q --detach "$SHA"

# Рецепт контура и четыре TM-DAG должны быть в самом дереве: compose монтирует их из
# ${TRANSFERMARKT_RELEASE_ROOT}, пустышек и симлинков нет.
for f in dags/dag_ingest_transfermarkt.py dags/dag_discover_transfermarkt_registry.py \
         dags/dag_backfill_transfermarkt.py dags/dag_transform_transfermarkt_silver.py \
         scrapers/transfermarkt/__init__.py scrapers/transfermarkt/client.py \
         scripts/proxy_filter/filter_proxy.py \
         deploy/transfermarkt/.airflowignore deploy/transfermarkt/airflow.compose.yaml \
         deploy/transfermarkt/gateway.compose.yaml deploy/transfermarkt/env.sh \
         deploy/transfermarkt/deploy.sh deploy/transfermarkt/auto_deliver.sh \
         deploy/transfermarkt/postdeploy_checks.sh deploy/transfermarkt/rotate_state.sh \
         deploy/transfermarkt/freeze_release.sh; do
  [ -s "$TMP_TREE/$f" ] || { echo "ОШИБКА: в $SHA нет $f — коммит старше рецепта #1387" >&2; exit 1; }
done

GIT_SHA=$(git -C "$TMP_TREE" rev-parse --short=8 HEAD)
TREE="$TRANSFERMARKT_RELEASES_DIR/release-${GIT_SHA}"
[ -e "$TREE" ] && { echo "ОШИБКА: $TREE уже существует" >&2; exit 1; }
mv "$TMP_TREE" "$TREE"
trap - EXIT

# mktemp даёт корню 0700, а шлюз и планировщик бегут под uid 50000 (инцидент SofaScore 25.08).
# chown logs/ не нужен: логи и состояние контура живут вне дерева ($TRANSFERMARKT_RUNTIME_DIR).
chmod 755 "$TREE"

echo "дерево заморожено: $TREE (sha $GIT_SHA)"
echo "дальше: bash deploy/transfermarkt/deploy.sh $TREE"
