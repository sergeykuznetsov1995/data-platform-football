# Контур Transfermarkt (#1387)

Свой Airflow (`transfermarkt-airflow`: metadb, init, scheduler, UI по профилю `ui` на
127.0.0.1:8084) и свой платный шлюз (`transfermarkt-gw`: `transfermarkt_gw`,
`filter_proxy.py --source-mode transfermarkt-only`, без суточного бюджета). Код — из
замороженного дерева `/opt/transfermarkt/releases/release-<sha8>`; логи и состояние —
в `/root/transfermarkt-runtime`, вне дерева. В DagBag девять DAG. Текущие
паузы и запуски показывает метабаза изолированного Airflow.

## Основной сбор Bronze

В этом контуре `TM_INGEST_ENGINE=core` выбирает новый сборщик для
`dag_ingest_transfermarkt`. Время расписания — 07:00 МСК. При допуске в расписание
сначала сверяют свежие страницы, квитанции и строки в Bronze по каждому разделу.
После решения владельца проверяют первый автоматический цикл.

Для ручной проверки положите архив со штатным `manifest.json` в
`/opt/airflow/logs/transfermarkt-core/replays/<package>`. Передайте в параметры
ручного запуска имя архива `package`, код лиги `league` и сезон `season`.
Airflow читает местные страницы и возвращает число строк по разделам в XCom.

Для запуска по расписанию нужен утверждённый список объектов нового сборщика.
Его JSON содержит `valid_from` и `valid_until` с часовым поясом и массив
`scopes`. Каждый элемент задаёт `scope_id`, `league`, `season`, `edition_id`
и цели из `clubs`, `players`, `coaches` или `sections`. Команда
`python -m dags.scripts.stage_transfermarkt_bronze_cycle registry` сохраняет
снимок в `registry/<sha256>.json`. При допуске в расписание укажите этот
хеш в `TM_CORE_INVENTORY_SHA256` изолированного Airflow. Выполняйте команды
под рабочим пользователем Airflow на его общем томе логов, чтобы задачи
могли прочитать файлы, созданные с правами владельца.

При старте Airflow создаёт `plan.json` из всего закреплённого реестра.
После разрешённого сбора подкоманда `captures` принимает `--started-at`,
`--attempts` с исходным журналом и `--traffic` с отдельным счётчиком попыток
этого же запуска. Квитанция счётчика содержит `cycle_id` или `run_id`
планировщика и целое `request_attempts`. Перед записью в Bronze процесс заново
сверяет сохранённые оригиналы журнала и квитанции по их хешам. Готовые файлы находятся в
`/opt/airflow/logs/transfermarkt-core/cycles/<sha256(run_id)>/`.
Для полного охвата нужен полный реестр объектов. Получение свежих страниц
и передача журнала выполняются загрузчиком с разрешённым доступом к источнику.

Airflow ожидает `captures.json` до часа. Затем он сверяет список целей с
закреплённым реестром, читает оригиналы из `s3://warehouse/raw/transfermarkt` и проверяет
все цели до первой записи. Пропуск страницы, отказ сайта и просроченный ответ
останавливают задачу. На каждую запись сборщик проверяет квитанцию и прочитанные
из таблицы строки. Повтор использует исходное время пакета.

Для записи требуются включённое разрешение `TM_STANDING_POLICY_ENABLED`, точные
таблицы Bronze и обе служебные таблицы `transfermarkt_core_receipts_v1` и
`transfermarkt_core_pages_v1` в действующей политике. Политика версии 8
охватывает только 10 из 35 таблиц Bronze нового сборщика. Получение оригиналов
и публикация полного реестра целей требуют работающего разрешённого источника.
При откате штатная доставка возвращает предыдущий релиз и сохранённые паузы.

## Установка (один раз, ничего на общем стеке)

1. `docker network create transfermarkt-net`
2. `install -d -m 0755 /etc/data-platform /opt/transfermarkt/releases`;
   `install -d -m 0700 /root/transfermarkt-runtime/auto-deliver`;
   `install -d -o 50000 -g 0 -m 0750 /root/transfermarkt-runtime/logs /root/transfermarkt-runtime/gateway-state`
3. `/etc/data-platform/transfermarkt.env` (root, 0600) по `transfermarkt.env.example`:
   образы = образы контура SofaScore, `TRANSFERMARKT_AIRFLOW_DB_PASSWORD` и
   `TM_PROXY_CONTROL_TOKEN` — `openssl rand -hex 32` (токен обязан отличаться от
   `PROXY_FILTER_CONTROL_TOKEN` общего .env). Пул Decodo — только файлом
   `TRANSFERMARKT_PROXY_POOL_FILE`, сам JSON в env не кладётся.
4. Автомат: `install -d /usr/local/libexec/transfermarkt` и
   `install -m 0755 deploy/transfermarkt/{auto_deliver.sh,rotate_state.sh} /usr/local/libexec/transfermarkt/`,
   `install -m 0644 deploy/transfermarkt/env.sh /usr/local/libexec/transfermarkt/`.
5. Первое дерево: `bash deploy/transfermarkt/freeze_release.sh <master sha>`.

## Переезд (шаг 2, только по слову владельца, окно 11:00–03:00 UTC)

1. Проверка: в общей метабазе нет `running` рана TM; env-файл заполнен.
2. Пауза ingest и discover на общем планировщике; подтвердить SELECT `dag.is_paused`.
3. `mv` из `/root/data-platform-football/logs/` в `/root/transfermarkt-runtime/logs/`:
   `transfermarkt-approvals`, `transfermarkt-registry`, `transfermarkt-native-v2/cycles`;
   кэш страниц и леджер шлюза начинаются чистыми; `chown -R 50000 logs`.
4. Подъём:
   `docker compose -p transfermarkt-airflow -f <RELEASE>/deploy/transfermarkt/airflow.compose.yaml --env-file /root/data-platform-football/.env --env-file /etc/data-platform/transfermarkt.env up -d airflow-metadb airflow-init`,
   затем `bash <RELEASE>/deploy/transfermarkt/deploy.sh <RELEASE>` и
   `bash <RELEASE>/deploy/transfermarkt/postdeploy_checks.sh`.
5. Cron (копия `crontab -l` в `.prev-<дата>` до правки):
   `*/5 * * * * /usr/local/libexec/transfermarkt/auto_deliver.sh >> /root/watchdog/transfermarkt_auto_deliver_cron.log 2>&1`
   и `20 10 * * * /usr/local/libexec/transfermarkt/rotate_state.sh`;
   записать `<sha>` в `/root/transfermarkt-runtime/auto-deliver/transfermarkt-accepted`.

## Эксплуатация

- `deploy.sh <NEW> [<OLD>]`: коды 2 предпосылки, 4 контур занят (running-ран TM) или
  замок выката занят — бой не тронут, 5 шлюз, 6 импорт/приёмка, 7 паузы. Журнал —
  `/root/transfermarkt-runtime/deploy.log`. Откат = `deploy.sh` старого дерева.
- Автомат (#2039): в любой час, когда полоса свободна и до ближайшего прогона
  `dag_discover_transfermarkt_registry` хватает запаса (дренаж + доставка + откат), доставляет
  `origin/master`, при провале откатывает. Одна попытка на sha (защёлка
  `auto-deliver/transfermarkt-auto-deliver-attempted-<sha8>`); полоса не освободилась или
  deploy.sh rc=4 — повтор того же sha через 6 ч. Три провальные попытки подряд — выключатель
  `auto-deliver/transfermarkt-auto-deliver.off`. Итог каждой попытки —
  `auto-deliver/transfermarkt-window-<дата>T<ЧЧММ>.env`.
- Полоса — те из ingest/backfill, что на непрерывном расписании (`dag.timetable_description`
  «Continuous Transfermarkt current…»). DAG на cron (ingest без квалификации, 04:00 UTC)
  внешний: пока идёт его прогон, дренаж не начинается, его ближайший запуск входит в дедлайн.
  Тайм-аут дренажа и нехватка запаса — исход `busy` (не провал), sha откладывается на 6 ч;
  сбой заморозки, недоступный master или молчащая метабаза возвращают полосу сразу.
- Дренаж полосы: автомат ждёт границы порций (в контуре нет running-прогона
  и задач в работе), записывает прежние паузы в `auto-deliver/transfermarkt-drain.env`, ставит
  полосу на паузу и доставляет. Посреди порции не паузит: Airflow 2 не двигает прогон паузного
  DAG. Дренаж дольше часа, стартовавший registry/silver, выключатель или бой = master —
  паузы полосы возвращаются из файла.
- Флаг дренажа `gateway-state/delivery-drain-until` (в контейнерах —
  `/opt/airflow/gateway-state/delivery-drain-until`, epoch «до когда»): пока он действует,
  `TransfermarktCurrentTimetable` не допускает новых порций, а идущая дорабатывает свой
  допуск ≤ 45 мин. Автомат пишет его на `DRAIN_MAX` (1 ч) при старте дренажа и ловит конец
  порции опросом внутри тика (до 4 мин); убирает после выката (успех, откат, rc=4), при любом
  прерывании дренажа и при разборе оборванной доставки. Битый, просроченный или отсутствующий
  флаг расписание игнорирует; если автомат умер, флаг истекает сам.
- `auto_deliver.sh --drill-rollback` — учения отката на копии принятого дерева.
- После успешной доставки автомат возвращает прежние паузы DAG и слоты общего
  ingest-пула. TM-пулы сохраняют значения нового релиза из `airflow_pools`:
  при выключенной истории её пулы остаются нулевыми. При откате возвращаются
  все прежние слоты и паузы из снимка. Отказ итоговой проверки записывается
  как `ACCEPTANCE result=0|X predicate=...`; `0` — несовпадение условия,
  `X` — чтение не удалось или ответ некорректен. Эти записи не доказывают
  прохождение предметной приёмки источника.
- `rotate_state.sh --dry-run` — что удалила бы ротация.

## Current по сигналам (#1393)

Контракты сбора, квалификация tmapi, warm baseline и интерфейс измерителя
описаны в [TRANSFERMARKT-CURRENT-1393.md](../../docs/operations/TRANSFERMARKT-CURRENT-1393.md).
Без валидного файла квалификации остаётся прежний daily ingest. Включённый
current использует изолированный TM timetable plugin из
`/opt/airflow/dags/utils/transfermarkt_plugins`; новые порции не стартуют
00:15–03:00 UTC, чтобы сохранить окно штатной автодоставки 04–06 МСК.
Наличие файла или merge не подтверждает runtime-приёмку: нужны физические
Bronze receipts, метабаза scheduler и три дня измеренного покрытия.

## Подготовка потоков (#1398)

[Контракт потоков и канарейки](../../docs/operations/TRANSFERMARKT-STREAMS-1398.md)
описывает постоянные rate slots, резерв актуалки, замедление и миграцию permit
checkpoint. По умолчанию `TM_HISTORY_STREAMS=0`, `TM_REQUESTS_PER_MINUTE=12`:
доставка кода не включает историю. Канарейка и изменение живых ресурсов требуют
отдельно согласованного запуска; короткие и unit-прогоны не являются приёмкой.
