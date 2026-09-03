#!/usr/bin/env bash
# Заморозка дерева контура SofaScore под ротацию (#1218, #1155 этап 3).
# Использование: bash deploy/sofascore/freeze_release.sh <sha>
# Переменные — из $SOFASCORE_ENV_FILE (по умолчанию /etc/data-platform/sofascore.env):
#   SOFASCORE_SOURCE_REPO, SOFASCORE_RELEASES_DIR.
# Дальше: deploy.sh <дерево> (платного замера перед выкатом больше нет, #1245).
set -euo pipefail

# The frozen checkout is mounted read-only into containers running as uid
# 50000.  Agent shells can inherit umask 0077; without an explicit value git
# then creates 0700 directories and 0600 files that production cannot read.
umask 0022

SHA="${1:?sha коммита для заморозки}"
ENV_FILE="${SOFASCORE_ENV_FILE:-/etc/data-platform/sofascore.env}"
# shellcheck source=deploy/sofascore/env.sh
. "$(dirname "$0")/env.sh"
sofascore_load_env "$ENV_FILE" || exit 2
: "${SOFASCORE_SOURCE_REPO:?}" "${SOFASCORE_RELEASES_DIR:?}"

[ -d "$SOFASCORE_RELEASES_DIR" ] || { echo "нет каталога релизов $SOFASCORE_RELEASES_DIR" >&2; exit 1; }
TMP_TREE=$(mktemp -d "$SOFASCORE_RELEASES_DIR/freeze.XXXXXX")
trap 'rm -rf "$TMP_TREE"' EXIT

# Без --shared: дерево живёт месяцами и не должно зависеть от gc в исходном репо.
git clone -q "$SOFASCORE_SOURCE_REPO" "$TMP_TREE"
git -C "$TMP_TREE" checkout -q --detach "$SHA"

# Рецепт контура (deploy/sofascore/*) и мини-DAG должны быть в самом дереве:
# compose монтирует их из ${SOFASCORE_RELEASE_ROOT}, пустышек и симлинков больше нет.
for f in deploy/sofascore/airflow.compose.yaml deploy/sofascore/gateway.compose.yaml \
         deploy/sofascore/.airflowignore configs/sofascore/workload_policy.json \
         dags/dag_trigger_sofascore_daily.py \
         dags/dag_sofascore_manifest_maintenance.py; do
  [ -s "$TMP_TREE/$f" ] || { echo "ОШИБКА: в $SHA нет $f — коммит старше рецепта #1155" >&2; exit 1; }
done

# Имя: release-<gitsha8>. Отпечаток дерева больше не участвует в идентичности:
# бюджет задаёт статическая политика из того же коммита (#1245), а не платный замер,
# привязанный к отпечатку.
GIT_SHA=$(git -C "$TMP_TREE" rev-parse --short=8 HEAD)
TREE="$SOFASCORE_RELEASES_DIR/release-${GIT_SHA}"
[ -e "$TREE" ] && { echo "ОШИБКА: $TREE уже существует" >&2; exit 1; }
mv "$TMP_TREE" "$TREE"
trap - EXIT

# ⚠ mktemp даёт корню 0700, а шлюз и планировщик бегут под uid 50000: без этого
# контейнер шлюза падает в цикле «can't open file … [Errno 13] Permission denied»
# (инцидент 25.08, 12 рестартов подряд, деплой вышел по таймауту healthcheck).
chmod 755 "$TREE"

mkdir -p "$TREE/logs"
chown -R 50000:root "$TREE/logs"

echo "дерево заморожено: $TREE (sha $GIT_SHA)"
echo "дальше: bash deploy/sofascore/deploy.sh $TREE"
