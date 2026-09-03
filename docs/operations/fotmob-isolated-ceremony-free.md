# FotMob: изолированный контур без церемонии (рунбук на страницу)

Действует с **2026-08-11**. Описывает контур в том виде, в каком он реально работает,
а не процедуру доставки из [`fotmob-production.md`](fotmob-production.md) — та написана
под церемониальный путь (canary → activation → evidence), который в этом контуре
**выключен**: ни `FOTMOB_DEPLOYMENT_REPORT_PATH`, ни `FOTMOB_SHARED_DEPLOYMENT_REPORT_PATH`
в контейнере не заданы, поэтому `fotmob_ceremony_configured()` возвращает `False`.
Установившееся состояние (таблица шести DAG) в `fotmob-production.md` описано верно —
расходится только способ, которым в него приходят.

## Где что лежит

| Что | Значение |
|---|---|
| Compose-проект | `fotmob-airflow`, рецепт — `deploy/fotmob/isolated.compose.yaml` (#1155; живой контур на 03.09.2026 ещё поднят из `/root/fotmob-runtime/fotmob-airflow.compose.v2.yaml` — тот же рецепт с зашитыми путями, переезд — этап 4 ниже) |
| Контейнеры | `fotmob-airflow-scheduler` (LocalExecutor, воркеры — форки), `fotmob-airflow-metadb` |
| Метабаза | своя, том `fotmob_airflow_pgdata`. **Никогда не переключать на общий postgres** |
| Код | бинд-маунты `ro` из `${FOTMOB_RELEASE_ROOT}` (сейчас `/root/dpf-fotmob-930-runtime`): `dags`, `scrapers`, `scripts`, `configs/medallion`, `configs/fotmob`; `logs` — `rw` |
| Блок-лист чужих DAG | канонический текст — `deploy/fotmob/isolated.airflowignore`; хостовая копия `${FOTMOB_DAGBAG_IGNORE_FILE}` (сейчас `/root/fotmob-runtime/airflowignore-fotmob.v2`) file-bind'ом → `/opt/airflow/dags/.airflowignore` |
| Автомат доставки | `deploy/fotmob/{auto_deliver,b6_deliver,window_alert}.sh` + `env.sh`, cron `*/5`; пути и пины — `/etc/data-platform/fotmob.env` (`deploy/fotmob/fotmob.env.example`) |
| Порты наружу | нет. Общая с платформой только сеть `dp-storage` (Trino, SeaweedFS) |
| Маркеры | `FOTMOB_ISOLATED_STACK=1`, `ALERT_ENV=fotmob-isolated` |

`dag_orchestrate_fotmob` **существует только при `FOTMOB_ISOLATED_STACK=1`** — на общем
планировщике модуль отдаёт `dag = None`, поэтому код #1149 в master для общего стека
безопасен.

## Как выкатывается новый код

Ротации образа нет. Деплой = **подмена смонтированного дерева**:

```bash
git -C /root/dpf-fotmob-930-runtime checkout --detach <sha>
```

Только `checkout` и только внутри того же каталога. `rsync` / `mv` / `rm -rf` + пересоздание
меняют инод каталога, а бинд-маунт привязан к иноду на момент старта контейнера — контейнер
продолжит видеть старое дерево, и никакой интервал этого не исправит.

Руками так делать больше не нужно: подмену делает автомат `auto_deliver.sh` (cron `*/5`,
описан в разделе «Рецепт контура из одного источника») — в ночное окно он зовёт
`b6_deliver.sh apply`, который переставляет ветку `deploy/fotmob-b6-master` на
`FOTMOB_TARGET`, принимает результат метабазой и md5 целевого модуля, а при провале
откатывает на `FOTMOB_ROLLBACK_REF` и докладывает в Telegram. Ветки `deploy/*`, на которых
стояло дерево, **не подлежат удалению**. Точка отката до #1149 — тег
`fotmob-runtime-pre-1149` (`5852151f`, ветка `wip/fotmob-930-runtime-tree`).

## Рецепт контура из одного источника (#1155, этап 3)

До этапа 3 рецепт жил вне git: два compose-файла в `/root/fotmob-runtime` (v1 для metadb и
init, v2 для scheduler'а), блок-лист, мёртвая копия мини-DAG и три скрипта в корне `/root`
(автомат доставки, b6, сторож окна) с зашитыми путями и пинами. Теперь всё, что нужно, чтобы
поднять контур с нуля — compose, блок-лист, переменные, автомат доставки, сторож окна, —
лежит в `deploy/fotmob/`. Вне репозитория остаётся одно: раннер кампании истории
(`$FOTMOB_CAMPAIGN_DIR` — `driver.sh`, `state/`, `logs/`; сейчас `/root/fotmob_history_backfill`),
которым автомат управляет после доставки. Это временная кампания, не часть контура; без её
каталога автомат глушит себя (лог «КАТАЛОГ КАМПАНИИ … НЕ НА МЕСТЕ»), а не доставляет
вслепую — забрать раннер в репозиторий или отвязать от него автомат решает владелец.

| Часть | Файл в репозитории | На хосте |
|---|---|---|
| Compose-проект `fotmob-airflow` (metadb, init, scheduler, webserver по профилю `ui`) | `deploy/fotmob/isolated.compose.yaml` | `docker compose -p fotmob-airflow -f … --env-file "$FOTMOB_PLATFORM_ENV_FILE" --env-file /etc/data-platform/fotmob.env up -d --no-deps <сервис>` (после `fotmob_load_env`) |
| Блок-лист чужих DAG | `deploy/fotmob/isolated.airflowignore` | копия `${FOTMOB_DAGBAG_IGNORE_FILE}` вне дерева, file-bind в `/opt/airflow/dags/.airflowignore`; менять только `>>` или `cp` поверх |
| Переменные: пути машины, образы, пароль метабазы, пины выката | `deploy/fotmob/fotmob.env.example` → `/etc/data-platform/fotmob.env` (root, 0600) | второй `--env-file` для compose и единственный файл, который читают скрипты |
| Автомат доставки, b6, сторож окна | `deploy/fotmob/{auto_deliver,b6_deliver,window_alert}.sh` + `env.sh` | один каталог (например `/usr/local/libexec/fotmob/`), две строки cron |

Не путать с `deploy/fotmob/airflow.compose.yaml` + `deploy.py`: это церемониальный путь
(проекция DagBag, deployment report, evidence, сеть `dp-backend`, другой том метабазы). В
живом контуре он выключен с 11.08 и на этот рецепт не накладывается; его судьба — отдельное
решение.

### Единый источник истины

- **Дерево** — `${FOTMOB_RELEASE_ROOT}`: git-worktree, которое автомат переставляет
  `git checkout -B deploy/fotmob-b6-master <FOTMOB_TARGET>` под живыми bind-mount'ами.
  Контейнеры при доставке не пересоздаются.
- **Пины выката** — пять строк `FOTMOB_TARGET`, `FOTMOB_ROLLBACK_REF`, `FOTMOB_ROLLBACK_SHA`,
  `FOTMOB_NEW_CODE_FILE`, `FOTMOB_NEW_CODE_MD5` в `/etc/data-platform/fotmob.env`.
  `FOTMOB_TARGET` и `FOTMOB_ROLLBACK_SHA` — КОРОТКИЕ SHA (7–8 hex, как печатает
  `git rev-parse --short`): автомат сравнивает их с `--short HEAD` по префиксу, полный SHA
  не совпал бы никогда (скрипты с ним не стартуют). «Поднять пин» = переписать строки
  атомарно и с сохранением прав, вне окна 19:45–23:20 UTC и не в момент тика cron:

  ```bash
  umask 077
  sed -e 's/^FOTMOB_TARGET=.*/FOTMOB_TARGET=<sha8 цели>/' \
      -e 's/^FOTMOB_ROLLBACK_REF=.*/FOTMOB_ROLLBACK_REF=<sha8 прежней цели>/' \
      -e 's/^FOTMOB_ROLLBACK_SHA=.*/FOTMOB_ROLLBACK_SHA=<sha8 прежней цели>/' \
      -e 's|^FOTMOB_NEW_CODE_FILE=.*|FOTMOB_NEW_CODE_FILE=/opt/airflow/scrapers/fotmob/<модуль>.py|' \
      -e 's/^FOTMOB_NEW_CODE_MD5=.*/FOTMOB_NEW_CODE_MD5=<md5>/' \
      /etc/data-platform/fotmob.env > /etc/data-platform/fotmob.env.new
  chown root:root /etc/data-platform/fotmob.env.new && chmod 0600 /etc/data-platform/fotmob.env.new
  mv -f /etc/data-platform/fotmob.env.new /etc/data-platform/fotmob.env
  ```

  md5 цели — `git show <TARGET>:scrapers/fotmob/<модуль>.py | md5sum`. Скрипты не правятся и
  не расходятся с репозиторием.
- **Блок-лист** — `deploy/fotmob/isolated.airflowignore`. Тест
  `tests/unit/deploy/test_fotmob_contour_recipe.py` требует, чтобы в нём был КАЖДЫЙ чужой
  `dags/dag_*.py` и ни один из семи своих: новый чужой DAG на master без строки здесь — красный
  CI (до этого блок-лист сопровождался руками и был fail-open). Хостовая копия монтируется
  файлом ВНЕ дерева: file-bind из дерева протух бы по иноду при первом `checkout`, который
  меняет этот файл (та же мина, что file-bind `dag_trigger_fotmob_daily.py` 27.07). Когда
  репозиторный файл получил новые строки — дописать их в хостовую копию `>>`, затем сверить:
  `sha256sum "$FOTMOB_DAGBAG_IGNORE_FILE" "$FOTMOB_RELEASE_ROOT/deploy/fotmob/isolated.airflowignore"`.
- **Пути хоста и образы** — только `${VAR:?…}`, каждый bind длинным синтаксисом с
  `create_host_path: false`. Проверки: `tests/unit/deploy/test_fotmob_contour_recipe.py`
  (статика), `tests/integration/test_compose_validity.py -k fotmob` (рендер),
  `tests/unit/deploy/test_fotmob_delivery_scripts.py` (скрипты против заглушек).
- **Env-файл читается как compose**: `deploy/fotmob/env.sh` (`fotmob_load_env`) принимает
  только ключи `FOTMOB_*`, ничего не подставляет и не экспортирует; чтобы compose и скрипты
  не прочли один файл по-разному, значения с `$` и inline-комментарии ` #` загрузчик
  отвергает, пути пишутся без хвостового `/`. Файл — единственный источник: загрузчик
  сначала снимает ВСЕ `FOTMOB_*` из окружения (и экспортированные тоже), потом читает файл,
  поэтому унаследованное значение не переживёт файл без этого ключа. У compose окружение
  процесса старше `--env-file`, и экспортированный `FOTMOB_*` молча перекрыл бы файл — все
  команды compose ниже идут после `fotmob_load_env`, который такие переменные уже снял.
  Общий `.env` платформы compose получает первым `--env-file` из
  `FOTMOB_PLATFORM_ENV_FILE` (скрипты этот ключ не читают). Секреты платформы
  (fernet, S3, Trino, Telegram) — по-прежнему в общем `.env`; в `fotmob.env` — только пароль
  метабазы контура (`FOTMOB_AIRFLOW_DB_PASSWORD`: база инициализирована 17.07 значением общего
  `AIRFLOW_DB_PASSWORD` того дня, ротация общего `.env` не должна её ломать).

### Установка (один раз на хост)

Всё от root (env-файл 0600, docker, каталог состояния, cron — root'а). Порядок важен: env-файл
заполняется ПЕРВЫМ и дальше не перезаписывается (`[ -e … ] ||`), его значения загружаются в
текущий shell без export — на них ссылаются остальные команды.

```bash
install -d -m 0755 /etc/data-platform /usr/local/libexec/fotmob
[ -e /etc/data-platform/fotmob.env ] || install -m 0600 -o root -g root \
  deploy/fotmob/fotmob.env.example /etc/data-platform/fotmob.env
"$EDITOR" /etc/data-platform/fotmob.env                    # заполнить (короткие SHA, пути без хвостового /)
. deploy/fotmob/env.sh && fotmob_load_env /etc/data-platform/fotmob.env   # значения в этот shell, без export
install -m 0755 deploy/fotmob/auto_deliver.sh deploy/fotmob/b6_deliver.sh \
  deploy/fotmob/window_alert.sh /usr/local/libexec/fotmob/
install -m 0644 deploy/fotmob/env.sh /usr/local/libexec/fotmob/
test -f "$FOTMOB_TG_ENV"                                   # токен Telegram (0600) — общий с автоматом
test -x "$FOTMOB_CAMPAIGN_DIR/driver.sh" && test -d "$FOTMOB_CAMPAIGN_DIR/state" \
  && test -d "$FOTMOB_CAMPAIGN_DIR/logs"                    # раннер кампании — вне репозитория
[ -e "$FOTMOB_DAGBAG_IGNORE_FILE" ] || \
  install -D -m 0644 deploy/fotmob/isolated.airflowignore "$FOTMOB_DAGBAG_IGNORE_FILE"   # -D создаёт каталог; дальше только >>
install -d -m 0755 "$FOTMOB_STATE_DIR"                     # автомат каталог состояния не создаёт
install -d -o 50000 -g 0 -m 0775 "$FOTMOB_RELEASE_ROOT/logs"   # logs/ не в git; compose источники bind'ов не создаёт
```

Cron root'а (`crontab -e` под root; обе строки раз в 5 минут; окно и одноразовость — внутри
скриптов):

```
*/5 * * * * /usr/local/libexec/fotmob/window_alert.sh >> /var/log/fotmob-window-alert.log 2>&1
*/5 * * * * /usr/local/libexec/fotmob/auto_deliver.sh  >> /var/log/fotmob-auto-deliver-cron.log 2>&1
```

Первый запуск контура — три шага по одному сервису из того же файла, в том же shell (после
`fotmob_load_env`); дальше `up` нужен только при смене образа или лимитов. Всегда
`--no-deps`: без него `depends_on` пересоздаёт `airflow-init` (у metadb → init →
scheduler порядок обеспечивают `--wait` и явная последовательность команд):

```bash
fmc(){ docker compose -p fotmob-airflow -f deploy/fotmob/isolated.compose.yaml \
  --env-file "$FOTMOB_PLATFORM_ENV_FILE" --env-file /etc/data-platform/fotmob.env "$@"; }
fmc up -d --no-deps --wait airflow-metadb                       # ждёт healthy
fmc up --no-deps --exit-code-from airflow-init airflow-init     # разово, до «init complete»
fmc up -d --no-deps airflow-scheduler
# UI для приёмки (опционально): fmc --profile ui up -d --no-deps airflow-webserver → http://127.0.0.1:8084
```

### Переезд живого контура на этот рецепт (этап 4 #1155)

Предусловия нет: боевое дерево `2c7e469f` (ветка `deploy/fotmob-b6-master`) — предок master,
кода вне master в бою нет. Живой контур на 03.09.2026 поднят из
`/root/fotmob-runtime/fotmob-airflow.compose.v2.yaml` (scheduler) и `…compose.yaml` (metadb и
init — v1, с 17.07 не пересоздавались); рендер `isolated.compose.yaml` с боевыми значениями даёт
те же монты, env, command, healthcheck и лимиты, что у работающих контейнеров. Задуманные
отличия: `${VAR:?}` и `create_host_path: false`; init на v2-якоре (без v1-блок-листа и мёртвого
file-bind мини-DAG); пароль метабазы — своя переменная. Шаги — руками владельца, в ночное окно
(19:45–23:20 UTC), при остановленной кампании (`state/STOP` + выключатель автомата):

1. Заполнить `/etc/data-platform/fotmob.env` живыми значениями: `FOTMOB_RELEASE_ROOT=/root/dpf-fotmob-930-runtime`,
   образ `data-platform-airflow-scheduler:fbref-590579ef…`, `FOTMOB_POSTGRES_IMAGE=postgres:16-alpine`,
   `FOTMOB_AIRFLOW_DB_PASSWORD` = текущий `AIRFLOW_DB_PASSWORD` общего `.env`,
   `FOTMOB_DAGBAG_IGNORE_FILE=/root/fotmob-runtime/airflowignore-fotmob.v2`, пути автомата
   (`/root/fotmob_history_backfill`, `/root/watchdog/state`, `/root/watchdog/fotmob_auto_deliver.log`,
   файл `telegram.env`, `pytest` из `/root/.venvs/dpf-test`), пины — из
   строк `TARGET=`/`ROLLBACK_*`/`NEW_CODE_*` живого `/root/fotmob-auto-deliver.sh` (там они
   уже короткие SHA — переносить как есть).
2. Установить скрипты и cron по разделу «Установка» (env-файл уже заполнен — шаг с example
   его не тронет; `logs/` в живом дереве есть, `install -d` его не меняет; хостовая копия
   блок-листа уже на месте). Старые строки cron (`/root/fotmob-auto-deliver.sh`,
   `/root/watchdog/fotmob_window_alert.sh`) снять тем же root-`crontab -e`. Замок, маркеры и
   защёлки в `/root/watchdog/state` — те же файлы, автомат продолжит с текущего состояния
   (`fotmob-b6-accepted` = принятый SHA).
3. Пересоздать scheduler из рецепта (`fmc up -d --no-deps --force-recreate airflow-scheduler`
   из раздела «Установка», после `fotmob_load_env`) — только при нуле активных ранов ингеста и пустом
   `pgrep run_fotmob_scraper`; metadb не трогать (том `fotmob_airflow_pgdata` тот же). Init
   пересоздавать не обязательно; если пересоздаётся — только из этого файла (v1-блок-лист
   больше не вернётся).
4. Приёмка: `docker inspect fotmob-airflow-scheduler` — ровно семь bind'ов: шесть на
   `${FOTMOB_RELEASE_ROOT}/…` и один на `${FOTMOB_DAGBAG_IGNORE_FILE}`; `import_error=0`, семь
   активных DAG контура, `sha256sum` хостовой копии блок-листа = репозиторной.
5. После приёмки из `/root/fotmob-runtime` нужна только хостовая копия блок-листа;
   `fotmob-airflow.compose*.yaml`, `airflowignore-fotmob` (v1), `airflowignore-fotmob.v2.pre1149`,
   `dag_trigger_fotmob_daily.py` (мёртвая копия — в дереве лежит другой файл),
   `fotmob-production.env` (копия всех секретов платформы, контур её не читает),
   `fotmob-host-trino.env`, `delta-20260717/`, `fbref-geoip/` больше не нужны — это этап 5,
   решение владельца.

### Приёмка деплоя — только по метабазе

`airflow dags list` и `airflow dags list-import-errors` в Airflow 2.11.2 **строят свой
DagBag с диска** (`dag_command.py:448`, `:498`) и покажут «ровно 7 дагов, среди них
оркестратор» уже через секунду после `checkout`, когда планировщик ещё ничего не видел.
`airflow dags unpause` при отсутствующей строке печатает «No paused DAGs were found»
и выходит с **кодом 0** — неотличимо от успеха. Обе пробы как гейт не годятся.

```bash
CUT=$(docker exec fotmob-airflow-metadb psql -U airflow -d airflow -At -c "SELECT now()")
git -C /root/dpf-fotmob-930-runtime checkout --detach <sha>

# ждать появления/переразбора в метабазе (обход каталога — раз в 300 с)
until docker exec fotmob-airflow-metadb psql -U airflow -d airflow -At \
  -c "SELECT 1 FROM dag WHERE dag_id='dag_orchestrate_fotmob'" | grep -q 1; do sleep 10; done

docker exec fotmob-airflow-metadb psql -U airflow -d airflow -c \
  "SELECT dag_id, is_paused, has_import_errors, last_parsed_time > timestamptz '$CUT' AS reparsed
     FROM dag ORDER BY dag_id"
docker exec fotmob-airflow-metadb psql -U airflow -d airflow -At -c "SELECT count(*) FROM import_error"
```

Приёмка: 8 строк (7 fotmob-дагов, включая `dag_collect_fotmob_players` с 25.08, + неактивный
легаси `dag_accept_fbref_bronze`), у всех семи `has_import_errors=f` и `reparsed=t`,
`import_error=0`, иноды маунтов не изменились.

После `unpause` проверять **данными**, а не кодом выхода:

```bash
docker exec fotmob-airflow-metadb psql -U airflow -d airflow -At \
  -c "SELECT is_paused FROM dag WHERE dag_id='dag_orchestrate_fotmob'"   # должно быть ровно: f
```

## Как понять, что контур жив

Единственная честная проверка — **прирост бронзы**, не цвет рана:

```bash
/root/.claude/bin/trino-ro.sh "SELECT target_type, status, count(*) FROM iceberg.bronze.fotmob_ingest_manifest \
  WHERE completed_at > current_timestamp - interval '30' minute GROUP BY 1,2 ORDER BY 3 DESC"

# долг: сыграно, деталей нет
/root/.claude/bin/trino-ro.sh "SELECT count(*) FROM iceberg.bronze.fotmob_matches_current m \
  LEFT JOIN iceberg.bronze.fotmob_match_payloads_current p ON p.match_id = m.match_id \
  WHERE p.match_id IS NULL AND m.cancelled = false \
  AND m.utc_time < to_iso8601(current_timestamp AT TIME ZONE 'UTC')"

docker exec fotmob-airflow-scheduler pgrep -af run_fotmob_scraper.py   # живой процесс сбора
```

Лог скрапера тонет в `InsecureRequestWarning` (по две строки на запрос) — фильтровать:

```bash
docker exec fotmob-airflow-scheduler bash -lc \
 "grep -av 'InsecureRequestWarning\|warnings.warn' \
  '/opt/airflow/logs/dag_id=dag_ingest_fotmob/run_id=<RUN>/task_id=scrape_fotmob_data/attempt=1.log' | tail -40"
```

Автоматический сигнал простоя стоит в `/root/watchdog/morning_report.py` (`fotmob_idle()`):
возраст последней записи манифеста (порог 3 ч) и долг за 7 дней.

## Расписание

Оркестратор `*/5 * * * *`. Фоновая полоса стартует до **13:30 UTC**, кооперативный дедлайн
**13:45**. Дневная полоса стартует в окне **14:00–15:00 UTC**, кооперативный дедлайн **21:00**.
После 15:00 `choose_lane` отдаёт `background_window_closed` и до полуночи ничего не запускает.

Дедлайн — это мягкий стоп: скоупы за границей помечаются `deferred`, ран закрывается как
`partial_success` и приёмка это принимает. Единственная альтернатива дедлайну — жёсткий
`execution_timeout` (8 ч) у `scrape_fotmob_data`, то есть SIGTERM и красный ран. Отсюда
правило для потолков запросов в `_LANE_CAPS`: **потолок обязан быть достижим** внутри
`rpm × min(окно, 8 ч)`, иначе он декоративен и останавливать ран становится нечему.
Инвариант закреплён тестом `test_lane_request_caps_are_reachable`.

## Мины

1. **Ручным триггером оркестратор не проверить.** `_attest_owner_runtime`
   (`dag_orchestrate_fotmob.py:148-151`) требует `run_type='scheduled'`; ручной ран падает
   на первой же таске при `retries=0`.
2. **Оркестратор молчит, пока жив любой ран ингеста.** `_ingest_child_active()` (`:124-130`)
   смотрит на `dag_ingest_fotmob` в `queued`/`running` без фильтра по возрасту. Ран,
   застрявший в `queued` (например, дагу поставили паузу с висящим раном), блокирует
   контур **навсегда и без единого красного рана**. `dagrun_timeout` у ингеста не задан.
3. **Подмена дерева на ходу рвёт живой ингест.** Новый `season_data_available` требует
   XCom `validate_data['bronze_inputs_changed']`; отчёт, записанный старым кодом, его не
   несёт. Менять дерево только при нуле активных ранов ингеста И пустом `pgrep`.
4. **Легаси-даги как путь отката не работают.** `dag_trigger_fotmob_daily` на новом коде
   гарантированно красный: 10557/10558 классифицируются `excluded`, но остаются в
   опечатанной `FOTMOB_DAILY_COMPETITION_IDS` → `scope_validation.errors`.
5. **Инварианта покрытия нет.** `FOTMOB_DAILY_COMPETITION_IDS` в автоматической ветке не
   используется; 19 из 21 турнира исторической когорты конкурируют за бюджет с остальными
   по правилу stalest-first. Проверять руками.
6. **Диагностика #1149 не работает.** `scripts/fotmob_recover.py` требует
   `--deployment-report` (`required=True`), `fotmob_observe.py` его перехеширует — на
   ceremony-free контуре оба не запускаются. Разбор только через psql изолята и Trino.
7. **`docker compose up` без `--no-deps`** потянет `fotmob-airflow-init`, который вернёт
   протухший v1-блок-лист (пока контур поднят из старых файлов; из
   `isolated.compose.yaml` init берёт тот же якорь, что scheduler). Регламент —
   `/root/SHARED-STACK-PROTOCOL.md`.
8. **File-bind файла из дерева, которое переставляет `git checkout`,** протухает по иноду:
   git пишет изменённый файл заново, контейнер держит старый. Поэтому блок-лист монтируется
   из хостовой копии вне дерева и правится только `>>`; в дерево его файл-бинд не переносить.

## Известные дефекты кода (issue #1159)

**Вечная краснота ранов и ручной Silver — ИСПРАВЛЕНО (11.08, PR #1161).** Ран непрерывной
полосы больше не краснеет из-за незакрытого каталога: обойти ~450 скоупов за одно окно
физически нельзя, поэтому отложенный на повтор скоуп — штатное промежуточное состояние.
Красным ран остаётся на неустранимом (`terminal`, жёсткие ошибки операций, отсрочка без
бюджетного основания) и на **застое** — когда ран не закрыл ни одного скоупа. Раз ран
может быть зелёным, `advance_fotmob_scheduler_state` и `trigger_silver_transform`
выполняются: **Silver триггерится сам**, курсор полос двигается.

**Журнал прогресса под контентной подписью — ИСПРАВЛЕНО (11.08, дефект 2).** Подписей
теперь две. Контрактная (контентная) осталась доказательством в отчёте: она меняется, как
только источник открыл новый сезон, и обязана меняться. Журнальная
(`deterministic_plan_signature` от сущностей и политики полосы) стабильна между ранами, и
именно под ней лежат попытки, завершения и резюме бэкфила. Следствия: полоса истории копит
прогресс, «дыру источника подтверждаем со второй попытки» доходит до второй попытки, а
обход продолжается с места остановки, а не с головы очереди.

Сопутствующее в той же правке:

- **терминальный исход больше не вечен** — TTL 24 ч (`planner.TERMINAL_RETRY_AFTER`), иначе
  одна сорвавшаяся запись завершения вычёркивала бы скоуп из обхода навсегда, и притом молча;
- **отчёт ограничен контрактом** — `selection.scope_attempts` и синтез отсрочек считаются по
  скоупам текущего каталога и исходам этого рана, чужая история в доказательства не лезет;
- **карта попыток читается один раз за ран** — до этого полный оконный запрос по манифесту
  повторялся на каждую попытку.

Одноразовый эффект переезда: первый ран после доставки видит пустой журнал (старые записи
лежат под контентными подписями) и начинает копить заново; полоса `backfill` в этот раз
перезаберёт часть уже собранного.

Что осталось открытым:

- **Журнальные записи коммитятся по одной строке.** За дневную волну 11.08 — ~900 коммитов
  Iceberg по 1–2 строки (`fotmob_ingest_manifest`, `fotmob_competition_scope_observations`),
  каждый ~9 с, то есть ~2 часа окна уходит на журнал, а не на источник. Данные сущностей
  при этом пишутся пачками (13713 и 50 строк за коммит).

## Откат (порядок обязателен)

Пауза оркестратора **не** останавливает уже запущенного ребёнка — он живёт до дедлайна
или до 8-часового `execution_timeout`. Менять дерево под живым процессом нельзя.

**Пометка рана `failed` в метабазе процесс НЕ останавливает** — она меняет ярлык, а
`run_fotmob_scraper.py` продолжает писать бронзу до 8-часового таймаута. Поэтому UPDATE
идёт ПОСЛЕ гейта, а не до: иначе гейт «0 активных ранов» удовлетворяет сам себя, оператор
верит ему и меняет дерево под живым процессом.

```bash
# R0 — заморозить источник новых волн
docker exec fotmob-airflow-scheduler airflow dags pause dag_orchestrate_fotmob

# R1 — погасить ЖИВОГО ребёнка. Пауза дага его не убивает: даг и ран независимы.
docker exec fotmob-airflow-scheduler airflow dags pause dag_ingest_fotmob
docker exec fotmob-airflow-scheduler pkill -f run_fotmob_scraper.py || true

# R2 — ГЕЙТ. Порядок обязателен: сначала процесс, потом метабаза.
docker exec fotmob-airflow-scheduler pgrep -af run_fotmob_scraper.py    # ждём пусто (rc=1)
docker exec fotmob-airflow-metadb psql -U airflow -d airflow -At -c \
  "SELECT count(*) FROM dag_run WHERE dag_id='dag_ingest_fotmob' AND state IN ('running','queued')"

# R3 — только теперь прибрать ярлыки и вернуть дерево
docker exec fotmob-airflow-metadb psql -U airflow -d airflow -c \
  "UPDATE dag_run SET state='failed', end_date=now() WHERE dag_id IN ('dag_ingest_fotmob','dag_orchestrate_fotmob') AND state IN ('running','queued')"
git -C /root/dpf-fotmob-930-runtime checkout wip/fotmob-930-runtime-tree   # 5852151f

# R4 — ВЕРНУТЬ СБОР. Одного dag_ingest_fotmob мало: у него schedule=None, он только
# триггерится. На старом дереве владелец — dag_refresh_fotmob (@continuous).
docker exec fotmob-airflow-scheduler airflow dags unpause dag_ingest_fotmob
docker exec fotmob-airflow-scheduler airflow dags unpause dag_refresh_fotmob
docker exec fotmob-airflow-metadb psql -U airflow -d airflow -At -c \
  "SELECT dag_id,is_paused FROM dag WHERE dag_id LIKE '%fotmob%' ORDER BY 1"
```

Без шага R4 откат заканчивается полностью простаивающим контуром, и это не даёт ни одной
ошибки — ровно та беззвучная поломка, ради которой заведён сигнал простоя в стороже.

`.airflowignore` при откате **не трогать**: строка `^dag_transform_espn_silver\.py$`
блокирует файл, которого в дереве `5852151f` не существует, а запись через `>` на
секунду обнуляет файл для живого DagFileProcessor.

После отката сбор снова мёртв — сразу решать, чем закрывать долг: на старом дереве
у `dag_refresh_fotmob` расписание `@continuous`, достаточно снять с него паузу.
