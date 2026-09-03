# SofaScore production: изолированный контур из одного источника

> Рецепт контура SofaScore (проекты `sofascore-airflow` и `sofascore-gw`, сторож аренд,
> ротация релиза) целиком живёт в `deploy/sofascore/`. До #1155 (этап 3) он был
> размазан по `/root/sofascore-runtime` (три compose-override вне git, два мини-DAG,
> `.airflowignore`, скрипты pkg2) и по drop-in'у systemd. Здесь — как контур устроен
> и как его ротировать. Первый переезд живого контура на этот рецепт — этап 4 #1155
> (раздел «Переезд живого контура» внизу).

## Из чего состоит контур

| Часть | Файл в репозитории | Compose-проект / unit | Что монтирует |
| --- | --- | --- | --- |
| Планировщик + своя metadata-DB | `deploy/sofascore/airflow.compose.yaml` | `sofascore-airflow` (`sofascore-airflow-scheduler`, `sofascore-airflow-metadb`, `airflow-init`, `airflow-webserver` по профилю `ui`) | код — из `${SOFASCORE_RELEASE_ROOT}`; состояние кампании, verified-артефакт, пул прокси, venv-шим — из runtime-каталога |
| Платные шлюзы полос (3 шт.) | `deploy/sofascore/gateway.compose.yaml` | `sofascore-gw` (`sofascore_gw_951` с алиасом сервиса `sofascore_proxy_filter`, `sofascore_gw_history`, `sofascore_gw_players` в сети `sofascore-net`) | всё дерево релиза в `/opt/sofascore-repo:ro`; артефакт и fallback-файл общие, WAL/ledger — свой каталог у каждого |
| Блок-лист DagBag | `deploy/sofascore/.airflowignore` | накрывает `dags/.airflowignore` внутри scheduler'а | — |
| Мини-DAG контура | `dags/dag_trigger_sofascore_daily.py`, `dags/dag_sofascore_manifest_maintenance.py` | обычные файлы `dags/`; на общем scheduler'е спрятаны через `dags/.airflowignore` | — |
| Сторожа аренд (3 шт.) | `deploy/sofascore/gateway_lease_watchdog.py` + `systemd/sofascore-gw-lease-watchdog{,-history,-players}.service` | одноимённые unit'ы, все читают `/etc/data-platform/sofascore.env` | — |
| Ротация | `freeze_release.sh` → `run_canary.sh` → `deploy.sh` → `postdeploy_checks.sh` | — | — |
| Переменные | `deploy/sofascore/sofascore.env.example` → `/etc/data-platform/sofascore.env` | второй `--env-file` после общего `.env` платформы | — |

Активны в контуре ровно пять DAG: `dag_ingest_sofascore`, `dag_backfill_sofascore_all_mens`,
`dag_refresh_sofascore_all_mens`, `dag_trigger_sofascore_daily` (14:00 UTC, триггер
ежедневника), `dag_sofascore_manifest_maintenance` (воскресенье 05:00 UTC). Остальные
файлы `dags/` блок-лист не пускает в DagBag (движок RE2, lookahead не работает —
поэтому блок-лист, а не allow-list).

## Три полосы источника (#1244)

С 03.09.2026 кампания истории, актуалка и дейли не делят один слот аренды: до развода
`--max-active-leases 1` на общем шлюзе давал `HTTP 429: paid-proxy concurrency limit
reached` почти на каждом запуске истории (03.09 за сутки — 105 упавших запусков
`run_historical_scope` против 3 успешных).

| Полоса | Шлюз (сервис / контейнер) | Пул Airflow | Дневной потолок |
| --- | --- | --- | --- |
| Актуалка + дейли | `sofascore_proxy_filter` / `sofascore_gw_951` | `ingest_scraper_pool` | 600 МБ |
| Кампания истории | `sofascore_gw_history` / `sofascore_gw_history` | `sofascore_history_pool` | 2000 МБ |
| Профили игроков | `sofascore_gw_players` / `sofascore_gw_players` | `sofascore_players_pool` | 400 МБ |

Сумма дневных потолков — труба источника, 3 ГБ/сутки. Потолок и число активных аренд у
каждой полосы — переменные окружения (`SOFASCORE_{PROXY,HISTORY_GW,PLAYERS_GW}_{DAILY_BUDGET_MB,
MAX_ACTIVE_LEASES}`), слоты пулов — `SOFASCORE_{HISTORY,PLAYERS}_POOL_SLOTS`: расширение
полосы правит `/etc/data-platform/sofascore.env`, а не рецепт. Пулы ставит `airflow-init`
при первом подъёме и шаг `pools` в `deploy.sh` на каждой ротации (init на ротации не
пересоздаётся). У каждого шлюза СВОЙ каталог состояния (`gateway-state`,
`gateway-state-history`, `gateway-state-players`): WAL и ledger рассчитаны на
единственного писателя. Артефакт бюджета, пул прокси и токен контрольной плоскости —
общие: runtime-контракт у трёх шлюзов один.

## Единый источник истины

- **Код** — одно замороженное дерево `${SOFASCORE_RELEASE_ROOT}`
  (`${SOFASCORE_RELEASES_DIR}/release-<digest8>-<gitsha8>`: digest = `runtime_fingerprint`
  дерева, идентичность runtime-контракта — по нему ищутся канарейка и артефакт; sha
  различает деревья с одинаковым контрактом, например правку только рецепта или мини-DAG,
  которые в fingerprint не входят).
  Оба compose-файла берут из него всё: `dags/`, `scripts/`, `scrapers/`, `configs/*`,
  `docker/`, `deploy/sofascore/.airflowignore`. Пустышек-mountpoint'ов и симлинков в
  дереве больше нет — `freeze_release.sh` их не создаёт, а проверяет, что рецепт есть
  в самом коммите.
- **Какое дерево в бою** — одна строка `SOFASCORE_RELEASE_ROOT=` в
  `/etc/data-platform/sofascore.env`. Её (и `SOFASCORE_PROXY_BUDGET_ARTIFACT_HOST/_ID`)
  переписывает `deploy.sh`; compose, сторож и приёмка читают тот же файл. Drop-in'ов
  systemd и `sed` по `/etc` нет.
- **Пути хоста и образы** — только переменные без дефолта (`${VAR:?…}`): без значения
  `docker compose config` падает, а не подставляет чужое дерево; каждый bind — длинный
  синтаксис с `create_host_path: false` (отсутствующий источник = ошибка запуска, а не
  молча созданный пустой каталог). Полный список — `sofascore.env.example`; проверка, что
  compose не зашивает `/root`, `/tmp`, секреты и override-теги —
  `tests/unit/deploy/test_sofascore_contour_recipe.py`; рендер —
  `tests/integration/test_compose_validity.py` (`-k sofascore`); прогон `deploy.sh`
  против заглушек docker/systemctl — `tests/unit/deploy/test_sofascore_deploy_script.py`.
- **Env-файл читается как compose, не как shell**: `deploy/sofascore/env.sh`
  (`sofascore_load_env`) снимает внешние кавычки и ничего не подставляет и не экспортирует —
  compose получает значения только через `--env-file`, а `deploy.sh` дополнительно
  передаёт дерево/артефакт этого выката явно, чтобы устаревшее значение из окружения
  оператора не перекрыло перепинованный файл. JSON пула — в одинарных кавычках.
- **Секреты** — общий `.env` платформы (fernet, S3, Trino, Telegram, control-token)
  плюс контурные значения в `sofascore.env` (пароль метабазы контура
  `SOFASCORE_AIRFLOW_DB_PASSWORD`, платный пул `SOFASCORE_PROXY_POOL_JSON`). В git —
  ничего из этого.

Runtime-каталог `${SOFASCORE_RUNTIME_DIR}` (вне git, переживает ротации): `all-men/`
(состояние кампании, rw), `gateway-state/` (WAL/ledger шлюза — единственный писатель шлюз),
`artifacts/<digest64>/proxy_budget_canary.json` (immutable verified-артефакты),
`canary-<digest8>/` (рабочие каталоги канареек), `legacy-scraper-venv/bin/python →
/usr/local/bin/python` (шим для пинованного образа), файлы пулов прокси.

## Установка (один раз на хост)

```bash
docker network create sofascore-net            # своя сеть контура; dp-storage создаёт основной проект
install -d -m 0755 /etc/data-platform /opt/sofascore/releases
install -m 0600 deploy/sofascore/sofascore.env.example /etc/data-platform/sofascore.env   # заполнить
install -m 0755 deploy/sofascore/gateway_lease_watchdog.py /usr/local/libexec/sofascore-gw-lease-watchdog
install -m 0644 deploy/sofascore/systemd/sofascore-gw-lease-watchdog.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now sofascore-gw-lease-watchdog.service
```

Метабаза и init поднимаются один раз из того же файла (дальше ротация трогает только
scheduler и шлюз):

```bash
docker compose -p sofascore-airflow -f deploy/sofascore/airflow.compose.yaml \
  --env-file "$SOFASCORE_PLATFORM_ENV_FILE" --env-file /etc/data-platform/sofascore.env \
  up -d airflow-metadb airflow-init
```

## Ротация релиза

Все скрипты читают `$SOFASCORE_ENV_FILE` (по умолчанию `/etc/data-platform/sofascore.env`).

1. `bash deploy/sofascore/freeze_release.sh <sha>` — клон из `SOFASCORE_SOURCE_REPO`,
   `checkout --detach`, digest дерева сверяется с шаблоном
   `configs/sofascore/proxy_budget_canary.json` того же коммита, дерево переезжает в
   `${SOFASCORE_RELEASES_DIR}/release-<digest8>-<gitsha8>` (0755, `logs/` под uid 50000).
2. `bash deploy/sofascore/run_canary.sh <дерево>` (в tmux) — отдельный шлюз из нового
   дерева (без боевого DNS-алиаса) + коллектор `SOFASCORE_CANARY_COLLECTOR_IMAGE`,
   холодные сэмплы по классам манифеста, `verify` → файл `VERIFIED` в
   `${SOFASCORE_RUNTIME_DIR}/canary-<digest8>/`. Бой не останавливается.
3. `bash deploy/sofascore/deploy.sh <дерево> [старое-дерево]` — вне окна 13:55–15:35 UTC:
   пауза обеих кампаний (`dag_backfill_sofascore_all_mens`, `dag_refresh_sofascore_all_mens`)
   и ожидание idle по метабазе (таски и DagRun ежедневника, истории, актуалки); сверка
   digest дерева с кандидатом; публикация артефакта в `artifacts/<digest64>/`; перенос
   состояния кампании (только если `state.json` ещё нет); `sofascore_runtime_preflight.py
   preflight`; перепин трёх строк в `sofascore.env`; `up -d --no-deps --force-recreate`
   scheduler'а и шлюза; ожидание healthy (10 мин), `scheduler-health`, `import_error=0`,
   ровно три активных core-DAG; история остаётся на паузе, актуалка возвращается в
   прежнее состояние; `systemctl restart` сторожа. Любой аварийный выход после паузы
   (preflight, compose, healthcheck, DAG-и) тоже возвращает актуалку и пишет в лог, на
   каком шаге встали и какое дерево записано в env-файле (перепин делается до `up`, так
   что после падения между scheduler'ом и шлюзом контур смешанный — повторить `deploy.sh`).
4. `bash deploy/sofascore/postdeploy_checks.sh` — только чтение, код выхода 1 при любой
   несошедшейся проверке: healthy и память шлюза, env кампании, точные пары
   «destination → source» всех монтов scheduler'а и шлюза (и ни одного лишнего монта
   поверх `dags/`), `import_error=0`, пять активных DAG контура, состояние кампании, пин
   и статус сторожа (`systemctl show -p ExecStart`), `/health` шлюза.

Всегда `--no-deps`: без него `depends_on` пересоздаст `airflow-init` (см.
`/root/SHARED-STACK-PROTOCOL.md`). Общий compose-проект `data-platform` эти команды не
трогают — у контура свои проекты.

## Мины, которые уже стреляли

- Дерево 0700 от `mktemp` → шлюз в цикле `Permission denied` (25.08); `freeze_release.sh`
  делает `chmod 755`.
- Файл-bind поверх read-only `dags/` без файла в дереве → Docker создаёт каталог, DAG
  молча исчезает (23.08, scheduler лежал 11 ч). Теперь мини-DAG — обычные файлы `dags/`.
- `docker update --memory 1g` шлюза терялся при пересоздании (23.08) — лимит 1 GiB
  живёт в `gateway.compose.yaml`.
- Порог `WAL_OOM_RISK_BYTES` сторожа 50 МБ замораживал его навсегда при лимите 1 GiB;
  боевое значение 300 МБ (с 24.08) возвращено в `gateway_lease_watchdog.py`.
- Общий `PROXY_POOL_JSON` из `.env` платформы — чужой пул; шлюз берёт только
  `SOFASCORE_PROXY_POOL_JSON` (fail-closed).
- Пароль метабазы контура ≠ общий `AIRFLOW_DB_PASSWORD` (база инициализирована 17.07
  своим значением); в compose он больше не зашит — `SOFASCORE_AIRFLOW_DB_PASSWORD`.

## Переезд живого контура на этот рецепт (этап 4 #1155)

> **Предусловие.** Живое дерево `dpf-release-6e91eb05` = коммит `04502731` локальной
> ветки `feat/sofascore-all-men-scaleout` (в `/root/data-platform-football`, на origin
> не запушена): 12 коммитов, которых нет в master — приоритет актуалки, валидация
> снапшота, очереди свежести (`dags/dag_refresh_sofascore_all_mens.py`,
> `dags/scripts/run_sofascore_schedule_refresh.py`, `dags/utils/sofascore_all_mens_state.py`,
> тесты, перештампованный `configs/sofascore/proxy_budget_canary.json`). Заморозка дерева
> из master до их мержа откатит логику актуалки. Сначала — PR этой ветки в master
> (`git merge-tree` конфликтов не показывает), потом этап 4.

Живой контур на 02.09.2026 поднят из `/root/sofascore-runtime/*.yaml` (цепочки вне git)
на дереве `/root/dpf-release-6e91eb05`; рендер `airflow.compose.yaml` и
`gateway.compose.yaml` с боевыми значениями даёт те же монты, env, command и healthcheck,
что у работающих контейнеров (проверено `docker compose config` против `docker inspect`;
отличия: `.airflowignore` берётся из дерева, file-bind'ов мини-DAG нет,
`create_host_path: false`). Шаги переезда, все — руками владельца в тихое окно:

0. Создать каталоги состояния новых шлюзов рядом с существующим:
   `<runtime-dir>/gateway-state-history` и `<runtime-dir>/gateway-state-players`
   (владелец и права — как у `<runtime-dir>/gateway-state`). Свой каталог у каждого
   шлюза обязателен: WAL и ledger рассчитаны на единственного писателя.
1. Заполнить `/etc/data-platform/sofascore.env` из живых значений
   (`SOFASCORE_RELEASE_ROOT=/root/dpf-release-6e91eb05`, артефакт `6e91eb05…`, каталоги
   `/root/sofascore-runtime/{all-men,gateway-state,legacy-scraper-venv}`,
   `SOFASCORE_PROXY_POOL_FILE=/root/fbref-949-runtime/proxys.txt`, fallback-файл,
   `SOFASCORE_AIRFLOW_DB_PASSWORD` — значение из строки `SQL_ALCHEMY_CONN` старого
   `sofascore-airflow.compose.yaml`, `SOFASCORE_PROXY_POOL_JSON` — значение из
   `.env.proxy-pool.decodo`, в одинарных кавычках; `SOFASCORE_POSTGRES_IMAGE=postgres:16-alpine`
   как у живой метабазы).
2. Заморозить дерево с коммитом, содержащим этот рецепт, прогнать канарейку, выкатить
   `deploy.sh` — первое дерево в `/opt/sofascore/releases/`.
3. Установить бинарь сторожа и ТРИ unit'а из репозитория (по одному на полосу:
   `sofascore-gw-lease-watchdog{,-history,-players}.service`), снять drop-in
   `/etc/systemd/system/sofascore-gw-lease-watchdog.service.d/override.conf`,
   `daemon-reload`, `enable` все три. Рестартовать их не нужно — это последний
   шаг `deploy.sh`.
4. После приёмки убрать из `/root/sofascore-runtime` старые compose/override, мини-DAG и
   `airflowignore-sofascore` (они больше не монтируются), а `pkg2/*.sh` заменить ссылкой
   на `deploy/sofascore/`.
