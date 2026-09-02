#!/usr/bin/env bash
# Заморозка дерева контура SofaScore под ротацию (#1218, #1155 этап 3).
# Использование: bash deploy/sofascore/freeze_release.sh <sha>
#   digest считается по дереву (scrapers.sofascore.runtime_fingerprint) и сверяется
#   с шаблоном configs/sofascore/proxy_budget_canary.json того же коммита.
# Переменные — из $SOFASCORE_ENV_FILE (по умолчанию /etc/data-platform/sofascore.env):
#   SOFASCORE_SOURCE_REPO, SOFASCORE_RELEASES_DIR, SOFASCORE_HOST_PYTHON.
# Дальше: run_canary.sh <дерево> (tmux), затем deploy.sh <дерево> после VERIFIED.
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
: "${SOFASCORE_SOURCE_REPO:?}" "${SOFASCORE_RELEASES_DIR:?}" "${SOFASCORE_HOST_PYTHON:?}"

[ -d "$SOFASCORE_RELEASES_DIR" ] || { echo "нет каталога релизов $SOFASCORE_RELEASES_DIR" >&2; exit 1; }
TMP_TREE=$(mktemp -d "$SOFASCORE_RELEASES_DIR/freeze.XXXXXX")
trap 'rm -rf "$TMP_TREE"' EXIT

# Без --shared: дерево живёт месяцами и не должно зависеть от gc в исходном репо.
git clone -q "$SOFASCORE_SOURCE_REPO" "$TMP_TREE"
git -C "$TMP_TREE" checkout -q --detach "$SHA"

# Рецепт контура (deploy/sofascore/*) и мини-DAG должны быть в самом дереве:
# compose монтирует их из ${SOFASCORE_RELEASE_ROOT}, пустышек и симлинков больше нет.
for f in deploy/sofascore/airflow.compose.yaml deploy/sofascore/gateway.compose.yaml \
         deploy/sofascore/.airflowignore dags/dag_trigger_sofascore_daily.py \
         dags/dag_sofascore_manifest_maintenance.py; do
  [ -s "$TMP_TREE/$f" ] || { echo "ОШИБКА: в $SHA нет $f — коммит старше рецепта #1155" >&2; exit 1; }
done

DIGEST=$(cd "$TMP_TREE" && PYTHONDONTWRITEBYTECODE=1 "$SOFASCORE_HOST_PYTHON" -B -c "
import sys; sys.path.insert(0,'.')
from scrapers.sofascore.runtime_fingerprint import runtime_fingerprint
print(runtime_fingerprint('.')['digest'])
")
TPL_DIGEST=$(python3 -c "import json;print(json.load(open('$TMP_TREE/configs/sofascore/proxy_budget_canary.json'))['runtime_fingerprint']['digest'])")
[ "$TPL_DIGEST" = "$DIGEST" ] || {
  echo "ОШИБКА: digest шаблона $TPL_DIGEST != fingerprint дерева $DIGEST (перештамповать bootstrap)" >&2
  exit 1; }

# Имя: release-<digest8>-<gitsha8>. digest — идентичность runtime-контракта (по нему
# ищутся канарейка и артефакт), sha различает деревья с одинаковым контрактом — правка
# только рецепта deploy/sofascore/ или мини-DAG (они в fingerprint не входят) получает
# новое дерево и не упирается в «уже существует».
GIT_SHA=$(git -C "$TMP_TREE" rev-parse --short=8 HEAD)
TREE="$SOFASCORE_RELEASES_DIR/release-${DIGEST:0:8}-${GIT_SHA}"
[ -e "$TREE" ] && { echo "ОШИБКА: $TREE уже существует" >&2; exit 1; }
mv "$TMP_TREE" "$TREE"
trap - EXIT

# ⚠ mktemp даёт корню 0700, а шлюз и планировщик бегут под uid 50000: без этого
# контейнер шлюза падает в цикле «can't open file … [Errno 13] Permission denied»
# (инцидент 25.08, 12 рестартов подряд, деплой вышел по таймауту healthcheck).
chmod 755 "$TREE"

mkdir -p "$TREE/logs"
chown -R 50000:root "$TREE/logs"

echo "дерево заморожено: $TREE (sha $GIT_SHA, digest $DIGEST)"
echo "дальше: bash deploy/sofascore/run_canary.sh $TREE  (tmux), затем deploy.sh после VERIFIED"
