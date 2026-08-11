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
| Compose-проект | `fotmob-airflow`, файл `/root/fotmob-runtime/fotmob-airflow.compose.v2.yaml` |
| Контейнеры | `fotmob-airflow-scheduler` (LocalExecutor, воркеры — форки), `fotmob-airflow-metadb` |
| Метабаза | своя, том `fotmob_airflow_pgdata`. **Никогда не переключать на общий postgres** |
| Код | бинд-маунты `ro` из `/root/dpf-fotmob-930-runtime`: `dags`, `scrapers`, `scripts`, `configs/medallion`, `configs/fotmob`; `logs` — `rw` |
| Блок-лист чужих DAG | file-bind `/root/fotmob-runtime/airflowignore-fotmob.v2` → `/opt/airflow/dags/.airflowignore` |
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

Ветка, на которую указывает дерево, **не подлежит удалению**: сейчас это
`deploy/fotmob-1149-isolated` (`94d0132a`). Точка отката — тег `fotmob-runtime-pre-1149`
(`5852151f`, ветка `wip/fotmob-930-runtime-tree`).

### Приёмка деплоя — только по метабазе

`airflow dags list` и `airflow dags list-import-errors` в Airflow 2.11.2 **строят свой
DagBag с диска** (`dag_command.py:448`, `:498`) и покажут «ровно 6 дагов, среди них
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

Приёмка: 7 строк (6 fotmob-дагов + неактивный легаси `dag_accept_fbref_bronze`), у всех
шести `has_import_errors=f` и `reparsed=t`, `import_error=0`, иноды маунтов не изменились.

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
   протухший v1-блок-лист. Регламент — `/root/SHARED-STACK-PROTOCOL.md`.

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
