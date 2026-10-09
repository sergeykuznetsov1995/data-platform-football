# deploy/espn

## `espn_stall_watch.py` — сторож простоя сбора ESPN (#1496)

Хостовый скрипт (cron `*/15`), не часть Airflow. Четыре правила, у каждого своя серия тревог в
Telegram:

| Правило | Когда тревога |
| --- | --- |
| `paused` | `EXPECTED_DAGS` (с #1507 — `dag_espn_current`) в metadb нового контура `espn-live-airflow-metadb-1` на паузе или отсутствует; metadb не отвечает — тревога того же правила «недоступен» |
| `stall` | в `BRONZE_TABLES` (с #1507 — `espn_match` нового bronze) нет ни одного матча с `_source_fetched_at` за 36 ч; Trino недоступен или таблицы ещё нет (до первой волны) — правило пропускается |
| `red:<slug>` (#1505) | турнир красный в 3 последних волнах `dag_espn_current` подряд по журналу волн `iceberg.ops.espn_wave_tournament_v1`; без issue; отбой — последняя волна с турниром зелёная или его нет ни в одной из 3 последних; журнала нет (до #1507) или Trino недоступен — правило молча пропускается |
| `downgrade` (#1506) | за 24 ч в журнале перепроверок `iceberg.ops.espn_recheck_v1` есть хотя бы один `downgrade_rejected` (ESPN прислал беднее — оставлено старое); текст — число и лиги; «продолжается» раз в сутки, без issue; отбой — за 24 ч случаев нет; журнала нет (до #1507) или Trino недоступен — молчит (`downgrade=no_recheck_log`) |

Серия: первая тревога → тишина, «⏳ продолжается N ч» раз в 24 ч → через 24 ч issue
(`source:espn,area:bronze,type:bug`, заголовок `ESPN: сторож [<правило>] — …`; открытая issue с
тем же заголовком не дублируется) + карточка Blocked (и для найденной открытой issue; не встала —
повтор каждым тиком без новой issue; в state эпизода `issue` и `blocked`) → «✅ отбой», когда
условие снято.
State — `/root/watchdog/state/espn_stall_state.json` (+ `.lock`), неподтверждённые Telegram-сообщения
ждут в `pending` и повторяются следующим тиком.

Контракт на #1504: реакция нового контура на сбой — красный турнир, никаких `pause_all` /
`on_failure → pause`; тревога на турнир, красный 3 волны подряд, — правило `red:<slug>` (#1505).

### Обновление установленного сторожа (#1505 и дальше)

Сторож уже стоит в cron (#1496): crontab не трогать, заменить только файл, сохранив прежний.

```bash
cp -p /root/watchdog/espn_stall_watch.py /root/watchdog/espn_stall_watch.py.prev-$(date +%Y%m%d)
cp deploy/espn/espn_stall_watch.py /root/watchdog/espn_stall_watch.py
PYTHONDONTWRITEBYTECODE=1 python3 -m py_compile /root/watchdog/espn_stall_watch.py
python3 /root/watchdog/espn_stall_watch.py --dry-run --state /tmp/espn-dry.json   # до #1507: red=no_wave_log downgrade=no_recheck_log
```

Откат: `cp -p /root/watchdog/espn_stall_watch.py.prev-<дата> /root/watchdog/espn_stall_watch.py`.
State `/root/watchdog/state/espn_stall_state.json` совместим в обе стороны (эпизоды `red:<slug>` и
`downgrade` старый файл просто не читает).

### Первая установка на хост (#1496, после мержа)

```bash
cp deploy/espn/espn_stall_watch.py /root/watchdog/espn_stall_watch.py
chmod 755 /root/watchdog/espn_stall_watch.py
python3 -m py_compile /root/watchdog/espn_stall_watch.py
python3 /root/watchdog/espn_stall_watch.py --dry-run --state /tmp/espn-dry.json   # --dry-run без --state запрещён
crontab -l > /root/watchdog/crontab.prev-$(date +%Y%m%d)
( crontab -l; echo '*/15 * * * * /usr/bin/python3 /root/watchdog/espn_stall_watch.py >> /root/watchdog/espn_stall_watch.log 2>&1' ) | crontab -
```

Первый боевой запуск даст две тревоги (stall и paused) — это правда про старый контур. Чтобы на
вторые сутки сторож не завёл issue-дубль эпика #1456 и не перевёл карточку эпика в Blocked, после
первого запуска пометить оба эпизода как уже эскалированные:

```bash
python3 - <<'PY'
import json, pathlib
p = pathlib.Path("/root/watchdog/state/espn_stall_state.json")
st = json.loads(p.read_text())
for ep in st["episodes"].values():
    ep.update(issue=1456, blocked=True)
p.write_text(json.dumps(st, ensure_ascii=False, indent=0))
PY
```

(`blocked: true` обязателен: без него сторож на вторые сутки поставил бы карточку #1456 в Blocked.)

Откат: `crontab /root/watchdog/crontab.prev-<дата>`; state можно удалить.

Ручной прогон: `--dry-run --state <отдельный файл> [--now 2026-09-25T22:00:00Z]` — печатает, что
отправил бы и какую issue завёл бы; Telegram и gh не трогает.

### Что менять дальше

- **#1507 сделано:** `METADB` = `espn-live-airflow-metadb-1`, `EXPECTED_DAGS` = `('dag_espn_current',)`,
  `BRONZE_TABLES` = `('espn_match',)`, `TS_COL` остался `_source_fetched_at`. Ставится на хост
  после посева нового контура (раздел «Доставка»), по «Обновлению установленного сторожа».

## Таблицы bronze нового контура (#1503)

Код: `scrapers/espn/bronze_schema.py` (DDL), `bronze_rows.py` (контракты парсера → строки),
`bronze_writer.py` (писатель пачки). Таблицы создаёт DAG нового контура (#1504) вызовом
`ensure_bronze_tables(IcebergWriter())`; в бою они появятся с первым прогоном (#1507).

| Таблица `iceberg.bronze.` | Строка | Ключ внутри партиции |
|---|---|---|
| `espn_match` | матч: счёт, счёт по таймам/доп. время/пенальти/сумма встреч, стадион, судья, формации, состояния частей | `event_id` |
| `espn_match_lineup` | игрок в матче, 15 статов, флаг `lineup_anomaly` | `event_id, team_id, athlete_id` |
| `espn_team_stats` | командная статистика: 28 статов числами | `event_id, team_id` |
| `espn_match_events` | keyEvents и commentary summary | `event_id, kind, event_key` |

- Партиции у всех — `competition_slug, season_year` (сезон — `source_season_year` ESPN). Никаких
  `scope_id`, поколений и легаси-ключей soccerdata (`ENG-Premier League`/`2627`): соответствие —
  в xref при разморозке витрин.
- Lineage у всех одинаковый: `_source='espn'`, `_ingested_at` (время коммита пачки, одно на пачку),
  `_batch_id` (один на пачку), `_source_fetched_at`, `raw_uri`, `raw_sha256`, `parser_version`.
  Сырьё не копируется в строки (`roster`, `extra_json`, `statistics_json` нет) — оно в raw store
  по `raw_uri`.
- Состояния на строке матча, не в служебном JSON: `disposition` (`captured` / `valid_empty` /
  `source_malformed` / `lineup_anomaly`, NULL до загрузки summary), `anomalies` (JSON-список
  классов), `reason`, `lineup_state` / `team_stats_state` / `events_state` (`captured` /
  `valid_empty` / `malformed`, `pending` до summary), `deep_state` (`pending` до волны 3),
  `first_fetched_at`, `rechecked_at` (NULL до перепроверки #1506), `first_published_at` и
  `status_checked_at` (измеритель свежести #1505, см. «Свежесть и DQ»).
- Запись: пачка = турнир-сезон = 4 коммита (lineup → team_stats → events → match; строка матча —
  последней, это признак завершённой публикации, #1504), каждый —
  `insert_dataframe_atomic(delete_filter="competition_slug=… AND season_year=… AND event_id IN (…)",
  single_statement_replace=True)`: один MERGE с надгробиями заменяет строки матчей пачки, без
  окна пустоты. Повтор пачки заменяет, а не дописывает; витринам дедуп не нужен.
  **Пачка несёт полное состояние каждого своего матча:** матч, пришедший без summary, теряет
  прежние дочерние строки — уже скачанный матч надо передавать вместе с разобранным summary.
- Имя `espn_lineup` в bronze занято легаси-таблицей soccerdata (её читает
  `dags/sql/silver/espn_lineup.sql`), поэтому состав — `espn_match_lineup`. `espn_match` и
  `espn_match_events` — тёзки таблиц silver (другая схема; мина #1156: в запросах всегда писать
  схему).

### Разморозка витрин ESPN — что переписать

Сейчас заморожено как есть: `*_generation_v2`, легаси `bronze.espn_lineup` / `espn_matchsheet` /
`espn_schedule`, SQL silver и их тесты. Переключение на новые таблицы — не одной правкой CTE
(C6-F9): silver выковыривает поля из JSON формы scoreboard, которых в новом bronze нет как JSON —
они колонки или отсутствуют.

| SQL silver | Читает сейчас | JSON-пути, которые придётся заменить |
|---|---|---|
| `espn_match.sql` | `espn_schedule_generation_v2`, `espn_matchsheet_generation_v2` | `$.sides.*.competitor.{advance,aggregateScore,shootoutScore,winner}`, `$.sides.*.team.venue.id`, `$.competition.{altGameNote,leg.value,series.completed}`, `$.season.{slug,type}`, `$.source.league.{name,season.displayName,season.startDate,season.endDate}`, `$.summaryGameInfo.officials[0].fullName`, `$.venue.address.{city,country}` |
| `espn_match_events.sql` | `espn_schedule_generation_v2` | `$.competition.details` (`athletesInvolved`, `clock`, `type`, флаги карточек/пенальти/автогола) → `espn_match_events` |
| `espn_team_match.sql` | `espn_schedule_generation_v2`, `espn_matchsheet_generation_v2` | `$.sides.*.competitor.statistics` (`name`/`displayValue`) → колонки `espn_team_stats` |
| `espn_player_match_aggregate.sql` | `espn_lineup_generation_v2`, `espn_schedule_generation_v2` | `$.plays` (`didScore`/`didAssist`), `$.subbedInFor/subbedOutFor.athlete.id`, `$.jersey` |
| `espn_substitutions.sql` | `espn_lineup_generation_v2`, `espn_schedule_generation_v2` | `$.subbedInFor.{athlete.id,athlete.displayName,jersey}`, `$.jersey` |
| `espn_venue.sql` | `espn_schedule_generation_v2` | `$.venue.address.{city,country}` (в новом bronze нет) |
| `espn_lineup.sql` | легаси `bronze.espn_lineup` | → `espn_match_lineup` |
| `espn_matchsheet.sql` | легаси `bronze.espn_matchsheet` | → `espn_team_stats` + поля матча из `espn_match` |

Ещё `xref_match.sql` читает легаси `bronze.espn_schedule` — его ключи тоже переводить на
`competition_slug`/`season_year`/`event_id`. Дедуп `ROW_NUMBER() … ORDER BY _ingested_at DESC`
в новых таблицах не нужен (одна строка на ключ); фикстура `tests/fixtures/bronze_schemas.json` и
`SILVER_MIN_ROWS` откалиброваны по старым таблицам.

## Волны актуалки — `dag_espn_current` (#1504)

Файл DAG — `deploy/espn/dags/dag_espn_current.py`, **не `dags/`**: новый файл в `dags/` меняет
ctime каталога, который пинует сторож WhoScored (`scrapers/whoscored/runtime_contract.py:311-319`),
и роняет его воркеры; к тому же волна 1 целиком живёт в проекте `espn-airflow` (допущение 8).
**#1507 подключает `deploy/espn/dags` как каталог DAG своего проекта `espn-live`** (раздел
«Доставка»; корень релиза — `/opt/airflow`, он же `PYTHONPATH`, чтобы импортировался `scrapers.espn`). DAG создаётся на паузе
(`is_paused_upon_creation=True`); первый живой прогон — после автодоставки #1507.

Расписание `0 0,6,12,18 * * *` (UTC), `max_active_runs=1`, `catchup=False`, `dagrun_timeout` 55 мин.
Логика — `scrapers/espn/wave.py` (чистые функции), DAG — обвязка.

| Задача | Что делает | Повторы |
|---|---|---|
| `prepare` | `ensure_bronze_tables(IcebergWriter())` + `ensure_journal_table` + `ensure_wave_log_table` + `ensure_recheck_table` (#1506) (идемпотентно, каждый прогон) | 2 |
| `plan_wave` | издания из кэша (лиги с `failed` — перечитываются) → статусы дня `all/scoreboard` за вчера и сегодня UTC (`force_refresh`) → сверка с `bronze.espn_match` → список турнир-сезонов с работой; в волну 00 — ещё проверка зависших POSTPONED/SUSPENDED старше 3 суток и сверка со списком core (#1505) | 2 |
| `run_tournament` | map по турнир-сезону (`max_active_tis_per_dag=4`): summary новых финалов (cache-first — одна загрузка на матч) → разбор → одна пачка `write_tournament_batch` → журнал запросов | 2, через 3 мин |
| `wave_summary` | `all_done`: таблица «турнир → состояние → первая ошибка», счётчики `withdrawn`/`moved`, время волны; журнал волн пишется **до** решения «волна красная» (#1505) | 0 |

- **В волну попадает турнир-сезон**, если у него есть финал без захваченного summary
  (`lineup_state = pending` или строки нет), новый матч или изменился статус/счёт/kickoff/серия
  пенальти (#1506). Уже захваченный неизменный матч в пачку не идёт; захваченный с изменившимся
  статусом идёт вместе со своим summary, перечитанным из raw store (пачка несёт полное состояние
  матча), через правило «не хуже» (см. «Перепроверка и «не хуже»»). В волну 00 добавляются
  перепроверки и выборка 5 %.
- **День ровно с 1000 событиями** (`all/scoreboard` без счётчика — признак обрезки) добирается
  `league_scoreboard_day` по всем 161 живым целям (≈ 2,7 мин на S0, редкий случай).
- **Пропавший матч — не авария.** Известный незавершённый матч, которого нет в ответе его дня:
  нашёлся в другом скачанном дне с другой датой → `disposition = moved` и новый kickoff; нет ни в
  одном дне → фиксированный `competitions/{id}/status`: 404 → `withdrawn`, известный статус →
  фиксированный core `events/{id}` для даты. После проверки event/league/year/team IDs и даты
  пишутся `moved`, актуальный kickoff и прочитанный статус; `moved` сам по себе не доказывает
  перенос. Неопубликованный незавершённый `moved` проверяется и за пределами scoreboard window.
  Подтверждённый будущий kickoff ждёт своего времени; отсутствующий `timeValid` не подтверждает
  время. Неизменная старая дата сохраняет долг. Неизвестный статус, ошибка запроса или metadata
  оставляют прежнюю строку и делают турнир красным. Metadata replay использует точные сохранённые
  raw bytes; refs из ответа не запрашиваются. Summary нужен только для настоящего финала и
  читается cache-first (существующая ограниченная перепроверка сохраняется). Больше
  `ESPN_WITHDRAWN_ALERT = 8` снятых за волну — предупреждение в итоге, не падение.
- **Цвет.** Упавший турнир красный с первой ошибкой (`<Тип>: <сообщение>`), соседи публикуются.
  Красным турнир становится и на планировании: нет ни одного издания (core не ответил —
  `failed` в кэше, перечитывается каждой волной), битое событие его лиги в ответе дня (день
  разбирается по турнирам, соседи целы), не удался добор обрезанного дня — такой турнир идёт в
  map с ошибкой и падает без повторов (`WavePlanError`); его матчи в этой волне не сверяются.
  Не удалась проверка статуса пропавшего матча (не 404) — матч не помечается, остальные
  запланированные матчи турнира пишутся, затем турнир красный с этой ошибкой. Битое тело дня
  (нет корневой лиги, не список событий, событие без лиги в `uid`) приписать турниру нельзя —
  падает `plan_wave`, волна красная (не «пустой зелёный день»).
- **Зависший POSTPONED/SUSPENDED**, по которому core ответил финалом, пишется с summary: счёт —
  из заголовка summary (ни один день его не показывает).
  Прогон красный, только если красных турниров > 20 % волны или упал `plan_wave`. Закрытая
  заслонка (`AllOriginsBlocked`/`LaneClosed`) — в `plan_wave` красная волна, в турнире — красный
  турнир, без повторов. Одиночный 403 (`OriginBlocked`) при проверке статуса, доборе дня или
  чтении издания — ошибка своего турнира, не волны. Алертов в Telegram из DAG нет:
  красный турнир виден в итоге `wave_summary` (строка «турнир → состояние → первая ошибка» в
  логе и XCom) и в журнале волн; тревога на турнир, красный 3 волны подряд, — правило
  `red:<slug>` сторожа (#1505).
- **Время волны** пишется в лог `wave_summary`; p95 (конец − старт) ≤ 60 мин за 3 суток меряется
  по полным границам прогона — `dag_run.end_date − dag_run.start_date` `dag_espn_current` в metadb
  `espn-airflow` (подготовка, ожидание пула и запись после последнего запроса входят); журнал
  `iceberg.ops.espn_request_journal_v1` (по `run_id`) — только разбивка сетевой части. Замер —
  после первого живого прогона, приёмка вехи 1 (#1505). Оценка пиковой волны: 2 статуса дня + 161
  `leagues/{slug}` (раз в сутки) + ≤ 175 summary (самый плотный замеренный день, 20.09) +
  проверки статусов, в день с обрезкой ещё 161 → ≈ 500 запросов ≈ 8–9 мин на S0 = 60/мин + запись
  (4 турнира параллельно).

| Env / объект | Значение |
|---|---|
| `ESPN_EDITIONS_STATE_PATH` | кэш изданий, по умолчанию `$AIRFLOW_HOME/state/espn/editions.json` (рядом с `gate.json`); старше 24 ч или потерян — пересчитывается из core (`leagues/{slug}` → `plan_editions`), потеря безвредна |
| `ESPN_GATE_STEP_CEILING` | ступень заслонки (S0 по умолчанию), полоса `live` |
| `ESPN_RAW_STORE_URI` | raw store транспорта (#1500) |
| пул `espn_live` | у `plan_wave` и `run_tournament`; 4 слота, объявлен в `deploy/espn/pools.json` (#1507) |

**Что включает #1507:** проект `espn-live` с каталогом DAG `deploy/espn/dags`, пул `espn_live`,
env выше, автодоставку; снятие паузы `dag_espn_current` — после посева; сторож
(`EXPECTED_DAGS`/`METADB`/`BRONZE_TABLES`) — там же. Раздел «Доставка».

## Свежесть и DQ (#1505)

Одно правило для утренней сводки, сторожей и приёмки вехи 1 — `scrapers/espn/criterion.py`
(docstring = определение; сводка копирует SQL дословно, менять только вместе с пакетом сводки).

- **Знаменатель** — строки `bronze.espn_match` целевых турниров (`senior_official`,
  `Denominator.targets()`, 163), `duplicate_of IS NULL`; единица — матч (R-40). Срок =
  `kickoff + 26 ч` (kickoff последний известный: перенос считается от новой даты); сутки D —
  UTC-день срока.
- **Вне знаменателя:** `disposition = 'withdrawn'` (отдельный счётчик) и не сыгранный матч,
  подтверждённый после kickoff: `terminal_nonplayed` или `STATUS_POSTPONED` при
  `status_checked_at > kickoff`.
- **Попал** — `played_final`, `lineup_state` и `team_stats_state` ∈ {captured, valid_empty},
  `first_published_at ≤ срок`. Всё остальное — промах, в том числе матч, чей статус не читался
  после `kickoff + 2 ч` (остановка сбора видна как промах, а не как пустой знаменатель).
- **Серия** — подряд сутки со сроками при ok/due ≥ 99 % (точное отношение); сутки без сроков
  нейтральны. Веха 1 = серия 3. Строка сводки: `• ESPN: сыгранных за сутки DD.MM N, ≤ 24 ч X %
  (ok/N), серия Y дн. (веха 1: 3 дня ≥ 99 %)`; при N = 0 — «нет сыгранных в новых таблицах».

Колонки `espn_match` для правила (#1505): `first_published_at` — момент перед коммитом строки
матча (после дочерних таблиц) в пачке, где матч впервые стал сыгранным с терминальными частями
(погрешность — один MERGE, не вся пачка); каждая следующая пачка переносит его из хранимой строки,
`_ingested_at` остаётся меткой последней пачки. Новый финал, чей summary не скачался, пишется без
summary (`pending`, промах измерителя) — матч не выпадает из знаменателя; турнир красный
(`SummaryFetchError`), задача повторяется. `status_checked_at` —
`fetched_at` тела дня (или момент ответа core), из которого взят статус. Несыгранный матч, чей
статус прочитан до `kickoff + 2 ч`, волна переписывает один раз со статусом, прочитанным после.

**Полнота знаменателя (R-09).** Волна 00 UTC читает список событий core
(`leagues/{slug}/events?dates=D-2..D`, `force_refresh`) по живым целям с открытым изданием в этом
окне (≤ 161 запрос в сутки); событие core, которого нет ни в bronze, ни в скачанных днях, ищется в
`league_scoreboard_day` своего турнира и планируется как обычный матч (`topup_days` турнира).
Событие, которого нет ни в одном дне лиги, — ошибка турнира (красный), не молчаливая потеря.

**Журнал волн** — `iceberg.ops.espn_wave_tournament_v1` (`scrapers/espn/wave_log.py`): строка на
турнир на волну (`run_id, wave_started_at, wave_finished_at, slug, season_year, state, matches,
first_error`) и строка самой волны `slug = '(wave)'`. Отсюда тревога `red:<slug>` сторожа и
p95 длительности волн за сутки в сводке (`criterion.WAVE_DURATION_SQL`, порог 60 мин —
критерий 3 #1504).

**DQ bronze** — `scrapers/espn/quality.py`, за вчерашние UTC-сутки (строки с `_ingested_at` в D):
доли `valid_empty` / `source_malformed` / `lineup_anomaly` по турниру (> 20 % при ≥ 5 матчах),
дубли `event_id`, `_ingested_at < _source_fetched_at` (R-18, должно быть 0), сыгранный без счёта,
загрузки summary на матч по журналу запросов (#1506: в среднем > 1,1 за 7 суток, > 2 на один матч
за 3 суток). В сводку — только нарушения, затем две строки перепроверки (см. ниже).

**Где строка.** Секция «🔵 ESPN» утренней сводки `/root/watchdog/morning_report.py`: строка
свежести заменяет временный измеритель #1496, ниже p95 волн и DQ. Пакет наложения —
`/root/espn-deliveries/1505/summary/` (ставит Fable после мержа; с #1506 —
`/root/espn-deliveries/1506/summary/`). До #1507 новых таблиц нет: строка честно пишет «нет
сыгранных в новых таблицах».

## Перепроверка и «не хуже» (#1506)

ESPN иногда дописывает матч позже (составы/судья/лента низших лиг — через 8–10 суток, 6 из 124
матчей) и иногда присылает урезанный ответ (13.08: 13 матчей `bra.copa_do_brazil` — игроки без
статистики, 0 командных статов при тех же keyEvents). Логика — `scrapers/espn/recheck.py`.

- **Одна загрузка summary на матч.** Захваченный матч перекачивается только по смене статуса,
  kickoff, счёта или серии пенальти (`wave._changed`; `parser_version` и остальной `extra_json` —
  не повод). Смена парсера — перечитывание тела из raw store (`replay_json`, 0 байт). Серия
  пенальти из дня (`competitor.shootoutScore` в `extra_json`) пишется в `home/away_shootout` поверх
  summary — иначе смена серии возвращала бы матч в каждую волну.
- **Перепроверка — одна, в волну 00, на kickoff + 7…10 суток**, только для неполных матчей:
  сыгранный, захваченный, `rechecked_at IS NULL`, и `disposition ∈ {valid_empty, lineup_anomaly}`
  или часть (`lineup_state` / `team_stats_state` / `events_state`) не `captured`, когда в той же
  лиге за 30 дней ≥ 50 % матчей эту часть имеют. Полный матч не перепроверяется никогда. Загрузка —
  `force_refresh`; `rechecked_at = now` при любом исходе (и `same`, и `failed`): второй нет.
  Неудачная загрузка (`failed`, в т.ч. HTTP 503) оставляет хранимый разбор, но турнир красный
  (`SummaryFetchError`). Повтор задачи или волны идёт по тому же плану (XCom, `planned_at`) и
  заново не качает ни перепроверку, ни выборку: пара (матч, вид) уже в журнале
  (`recheck.journalled`) — пропуск, а если там `failed` — турнир остаётся красным на каждом
  повторе; строки журнала нет (его запись упала после записи bronze), но raw store хранит тело,
  скачанное не раньше `planned_at`, — оно перечитывается (0 загрузок) и строка журнала
  дописывается; перепроверка с `rechecked_at` без такого тела — упавшая загрузка прошлой попытки:
  `failed` в журнал, турнир красный, без загрузки. Матч, которого нет в днях волны,
  собирается из строки bronze вместе с хранимой серией пенальти: исправленный днём счёт серии
  не затирается summary.
- **Выборка 5 %** — `event_id % 20 = 0` среди сыгранных и опубликованных, в первую волну 00 после
  `first_published_at` + 24 ч и + 72 ч (окно — сутки): журнал «когда ESPN дописывает»;
  `rechecked_at` не трогает. Перепроверка важнее выборки у одного матча.
- **Правило «не хуже»** — ко всякому повторному summary (перепроверка, выборка, смена статуса после
  захвата с изменившимся телом): новое и хранимое тела разбираются, по каждой части (строки
  состава, игроки со статистикой, заполненные командные статы, события) число единиц нового ≥
  старого → пишем новое (`filled` — стало богаче, `changed` — правки значений, `same`); хоть одна
  часть беднее → остаётся хранимый разбор, перечитанный по `raw_uri`/`raw_sha256`
  (`EspnRawStore.load_exact`), в журнал — `downgrade_rejected`. Сравнение — по разобранному, не по
  хешу байтов (ESPN меняет байты у 69 % ответов).
- **Журнал** `iceberg.ops.espn_recheck_v1` (создаёт `prepare`): строка на попытку — `checked_at,
  run_id, slug, season_year, event_id, kind` (`recheck` / `sample_24h` / `sample_72h` / `refresh` —
  смена статуса с новым телом), `before_parts` / `after_parts` (JSON частей; `NULL` после неудачной
  загрузки), `outcome` (`filled` / `same` / `changed` / `downgrade_rejected` / `failed`). Пишется
  после записи пачки: в журнале — то, что лежит в bronze.
- **Нормы загрузок** (DQ сводки, журнал запросов, `disposition = 'success'` на `url_fingerprint`):
  в среднем ≤ 1,1 за 7 суток, ≤ 2 на матч за 3 суток. Ожидание по построению: 1 + 0,05 × 2
  (выборка) + доля перепроверок; проверяется на живом журнале после #1507.
- **Сводка:** `• ESPN перепроверка DD.MM: downgrade_rejected за сутки: N` (с лигами, `‼️` при N > 0)
  и `• ESPN перепроверка DD.MM: дозаполнено при перепроверке: X из Y (Z %); по лигам: …`
  (`quality.build_recheck_sql` / `render_recheck_lines`). **Сторож:** правило `downgrade`.
- **Темп:** перепроверки и выборка идут по полосе `live` той же заслонки (S0).

## История — `dag_espn_history` (#1509)

Прошлые сезоны на остатке скорости актуалки. Логика — `scrapers/espn/history.py`, SQL и строка
сводки — `scrapers/espn/history_report.py`, DAG — `deploy/espn/dags/dag_espn_history.py`.

| Что | Как |
| --- | --- |
| расписание | каждые 30 мин (`*/30 * * * *`), `max_active_runs=1`, создаётся **на паузе** |
| задачи | `prepare` (таблицы bronze, журнал запросов, очередь) → `run_history` |
| пул | `espn_history`: 1 слот, `priority_weight=1`, `weight_rule="absolute"` (`pools.json`) |
| время | бюджет 12 мин от старта DAG-прогона (раннер выходит сам, запись пачки — после), таймаут задачи 18 мин, `dagrun_timeout` 20 мин, ретраев нет — следующий прогон через 30 мин; между прогонами истории ≥ 10 мин окна для доставки |
| заслонка | полоса `history`: только остаток сверх доли актуалки (`live_share` 0,5), при 403/429 замирает первой — `LaneClosed`, прогон выходит без ошибки |
| долг актуалки | перед каждой пачкой: есть матч живого турнира с kickoff 14…72 ч назад, который измеритель считает «в сроке», а он не опубликован, — прогон выходит до следующего |
| очередь | `iceberg.ops.espn_history_queue_v1`: строка на турнир × сезон × type (`pending → listed → done | red | empty`) + строка `(run)` на каждый прогон (причина выхода, матчей записано) |
| охват | `configs/espn/history_scope.json` |

Путь запросов: `leagues/{slug}/seasons?limit=100` (раз на турнир) → `seasons/{год}` (окно, types)
→ `types/{t}/events` все страницы `limit=100` → summary по id; строка расписания — из `header`
summary, scoreboard не нужен. Закрытый сезон (конец в core > 7 дн назад) читается из raw store:
повтор — 0 сетевых запросов. Пачка — до 400 матчей одного турнира-сезона-type; упавший матч
делает строку type красной, её пробует ещё один следующий прогон, дальше она остаётся красной.

**Включение** (после доставки автоматом, владельцем):

```bash
docker exec espn-live-airflow-scheduler-1 airflow dags unpause dag_espn_history
```

**Стоп-файл** — выключить историю, не трогая DAG и актуалку (проверяется в начале прогона и между
матчами; прогон выходит, записав собранное):

```bash
docker exec espn-live-airflow-scheduler-1 touch /opt/airflow/state/espn/history.off   # стоп
docker exec espn-live-airflow-scheduler-1 rm /opt/airflow/state/espn/history.off      # снова в работу
```

**Охват** — правкой `configs/espn/history_scope.json` в master (доставка — автоматом):

- `{"allow": [["eng.1", 2015]]}` — ровно эти турнир × сезон (так включено в #1509);
- добавить пары — список пополняется, новые сезоны встанут в очередь следующим прогоном;
- `{"allow": []}` — все целевые турниры (`Denominator.targets()`): прошлые сезоны, начавшиеся за
  последние 10 лет (лига — 10 сезонов, сборные — издания 10 лет); уже собранное не качается.

Строки очереди вне охвата ждут; сезон из `allow`, которого нет в core, — красная строка.

Проверка (read-only Trino):

```sql
SELECT slug, season_year, season_type, state, matches, done, failed, attempts, last_error
FROM iceberg.ops.espn_history_queue_v1 ORDER BY updated_at DESC LIMIT 20;
```

Строка утренней сводки — `history_report.render_history_line` (сезонов готово за сутки / всего,
красных, матчей, запросов и КБ на матч по журналу `lane='history'`, пауз из-за актуалки); в
живой `morning_report.py` её ставит Fable пакетом после мержа.

## Доставка (#1507)

Новый контур живёт в своём compose-проекте **`espn-live`** (`deploy/espn/airflow.compose.yaml`):
своя metadb (`espn-live-airflow-metadb-1`, том `espn_live_pgdata`, пароль — свежий, не из
сожжённых 10.08), scheduler + webserver (UI `127.0.0.1:8089`), тома `espn_live_logs` и
`espn_live_state` (→ `/opt/airflow/state`: заслонка `gate.json`, издания `editions.json`), сети
`dp-storage`/`dp-backend`. Старый проект `espn-airflow` (7 DAG на паузе, 8086) не трогается до
задачи 21 — тот же `-p` пересоздал бы его контейнеры. Общий стек и общий `compose.yaml` — не
трогаются.

- **Образ** — digest `data-platform-airflow-scheduler@sha256:5286d9cf…` прямо в compose
  (`pull_policy: never`): код и образ едут одним коммитом, пересборки нет.
- **Код** — неизменяемый корень релиза `/root/espn-release-<sha>` (`git archive <sha> --
  deploy/espn scrapers configs/espn`, права без записи). Монтируются **каталоги** ro:
  `deploy/espn/dags` → `/opt/airflow/dags`, `scrapers` → `/opt/airflow/scrapers`, `configs/espn` →
  `/opt/airflow/configs/espn`; `PYTHONPATH=/opt/airflow`. Файловых монтирований нет (rename при
  checkout теряет файловый bind-mount). Тест: замыкание импортов DAG ⊆ смонтированных каталогов.
- **Env** — `/root/.secrets/espn.env` (0600), только секреты: `AIRFLOW__CORE__FERNET_KEY`,
  `AIRFLOW__WEBSERVER__SECRET_KEY`, `ESPN_LIVE_DB_PASSWORD` (hex), `TRINO_PORT`, `TRINO_USER`,
  `TRINO_PASSWORD`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `ICEBERG_WAREHOUSE`. Никаких `*_PROXY`
  (транспорт ESPN падает `AmbientProxyError`), `ESPN_RELEASE_ROOT`, canary-state, контрольной БД,
  `RELEASE_COMMIT/TREE_SHA256` — автомат отказывается работать с таким env-файлом.
- **Пулы — кодом:** `deploy/espn/pools.json` (`espn_live`: 4 слота = `max_active_tis_per_dag`;
  `espn_history`: 1 слот, #1509;
  файл добавлен `git add -f` — `*.json` в `.gitignore`). `airflow-init` делает только
  `airflow db migrate` + `airflow pools import` + удаление пулов, которых нет в `pools.json`
  (кроме `default_pool`); запускается при посеве и когда `pools.json` цели отличается от живого.

### Автомат `auto_deliver.py`

Хост-копия `/root/espn-deploy/auto_deliver.py` (= master, иначе «обнови автомат» в Telegram и
стоп), cron `*/5`, лог — `/root/watchdog/espn_auto_deliver_cron.log`. Каталог проекта compose —
`/root/espn-deploy` (метка `working_dir` не держит дерево кода). Посеян 26.09.2026 (master e2f5ba59).
Состояние
`/root/espn-deploy/state/`:

| Файл | Смысл |
| --- | --- |
| `accepted` | живой SHA — **единственный пин**; корень релиза = `/root/espn-release-<accepted>` |
| `accepted-prev` | прежний принятый — цель `--rollback` |
| `rejected` | SHA, не прошедшие приёмку (по строке); master с тем же содержимым контура не выкатывается повторно |
| `inflight` | выкат идёт/оборвался; висит — автомат стоит и раз в сутки просит руки |
| `off` | выключатель: есть файл — автомат ничего не делает |
| `lock` | flock от параллельного запуска |
| `notified` | какие остановки уже ушли в Telegram сегодня (повтор — раз в сутки) |

Шаг cron: `fetch` → самопроверка → цель = `origin/master`, если он потомок `accepted`
(`merge-base --is-ancestor`; иначе стоп «нужны руки»), не в `rejected` и `git diff accepted..master`
задевает пути контура (`deploy/espn`, `configs/espn` + замыкание импортов DAG из
`deploy/espn/dags`, в т.ч. `scrapers/base/*` и относительные импорты) → окно: нет
running/queued `dag_espn_current` и `dag_espn_history` (#1509; прогон истории ≤ 20 мин, между прогонами ≥ 10 мин окна) и до
волны 00/06/12/18 UTC ≥ 10 мин → корень релиза →
[`run --rm --no-deps -T airflow-init`] → `up -d --no-deps --force-recreate airflow-scheduler
airflow-webserver` с `ESPN_RELEASE_ROOT=<корень>` в окружении вызова → приёмка ≤ 7 мин:

- `dag.last_parsed_time` каждого DAG корня (`deploy/espn/dags/dag_*.py`: `dag_espn_current`,
  `dag_espn_history`) позже момента выката, `has_import_errors = f`, `import_error` = 0 (свежая
  ошибка разбора — провал сразу); откат на корень без истории её не ждёт;
- `/opt/airflow/dags` scheduler смонтирован из корня цели; sha256 файлов `deploy/espn/dags` и
  `scrapers/espn` в контейнере = корню;
- пулы metadb (кроме `default_pool`) = `pools.json`: тот же набор, слоты и `include_deferred`;
- в env контейнера нет `*_PROXY`.

Успех → `accepted-prev ← accepted`, `accepted ← sha`, Telegram «✅ ESPN выкачен <sha>», уборка
корней (храним 3 новейших + `accepted`/`accepted-prev`; корень удаляется, только если на него не
ссылается метка `espn.release_root` ни одного контейнера). Провал → SHA в `rejected`, тот же `up`
на `accepted`, повторная приёмка, Telegram «❌ … откат принят» с причиной; откат не подтвердился —
`off` + Telegram «🆘 НУЖНЫ РУКИ».

Ручные режимы (из-под root, как cron):

```bash
python3 /root/espn-deploy/auto_deliver.py --target <sha|ветка>   # выкат потомка accepted вне фильтра путей (проверка отката)
python3 /root/espn-deploy/auto_deliver.py --rollback              # вернуть accepted-prev; текущий → rejected
touch /root/espn-deploy/state/off                                 # выключить; rm — включить
```

Окно и приёмка — те же, что у cron; вне окна ручной режим отказывается (стоп с причиной).

### Посев (один раз, после мержа)

Скрипт посева — вне репозитория (`/root/espn-deliveries/1507/seed.sh`): свежие пароль metadb,
Fernet и secret key; S3/Trino — копия значений из старого env-файла по именам; тома с владельцем
`50000:0` для `state`/`logs`; хост-копия автомата из master; затем

```bash
python3 /root/espn-deploy/auto_deliver.py --seed <sha мержа>
```

— `up -d --wait airflow-metadb` → `airflow-init` → scheduler/webserver → та же приёмка →
`accepted`. Дальше cron-строка:

```
*/5 * * * * /usr/bin/python3 /root/espn-deploy/auto_deliver.py >> /root/watchdog/espn_auto_deliver_cron.log 2>&1
```

`dag_espn_current` создаётся на паузе; снятие паузы — отдельным шагом после посева.

**Откат контура целиком:** `touch /root/espn-deploy/state/off`, `docker compose -p espn-live …
stop airflow-scheduler airflow-webserver` (поимённо). Старый `espn-airflow` так и стоит на паузе.

## #1510: контролируемый замер темпа

Рабочий модуль — `scrapers.espn.measure_pace`: scheduler монтирует `scrapers`/`configs`,
но не `deploy/espn`. `deploy/espn/measure_pace.py` — только обёртка для checkout.
Контроллер использует один неблокирующий lock и `pace.sqlite3` в
`/opt/airflow/state/espn` (`espn_live_state`), рядом с общей заслонкой и журналом
HTTP-попыток. Каталог автомата доставки не является состоянием замера.

Ступени: S0 60/мин, 1 worker, 2 ч; S1 120/мин, 2 workers, 24 ч;
S2 240/мин, 4 workers, 24 ч. Общая половинная квота истории — максимум
30/60/120 попыток в минуту. Контроллер повторно читает фиксированные 380 принятых
ID eng.1 2015, профиль только Summary; это не прирост исторического покрытия.
Все запросы идут через history lane общей заслонки с прежним User-Agent.
Raw замера сохраняется только в `measurement-raw/<measurement_id>` тома состояния;
очередь истории, производственный Bronze и first_published_at не изменяются.

Проверяются полные UTC-интервалы по 5 минут: >=80% квоты истории в >=90%
интервалов без известного долга/защитной паузы. Нет наблюдения, незавершённая
HTTP-попытка, пустое окно или неизвестная свежесть запрещают повышение.
Наблюдение долга/защиты — каждые 10 с от начала SQL, разрыв >30 с начинает новое окно.
Долг, суточная свежесть и публикация outbox выполняются в трёх независимых потоках,
каждый создаёт/закрывает свой Trino manager/connection; очередей заданий нет.
Возраст долга считается от начала чтения, а не от heartbeat или завершения SQL.
Если прежний результат успел устареть до следующего, окно начинается заново даже
при расстоянии между завершениями SQL меньше 30 с. Медленные freshness, публикация,
локальная агрегация и стенд не останавливают наблюдателя; медленный debt по-прежнему
запрещает HTTP и повышение. Только главный поток меняет состояние контроллера.
`observation`, `freshness_observation`, `publication` в JSON-статусе показывают
самостоятельные результаты/ошибки и длительности; `phase` в SQLite содержит
aggregation/http_drain/benchmark/sql_drain. Не заменять эти данные heartbeat.
Обычная публикация отправляет не более 200 dirty-версий за проход, сохраняет
flock/MERGE по attempt_id и подтверждает только точную отправленную версию.
Рестарт сохраняет ID, baseline, принятые ступени и события, но начинает новое
непрерывное окно. Heartbeat/события start/stop сохраняются; простой процесса не
считается доказательством нагрузки. Свежесть — точное >=99% по существующему
UTC-измерителю; окончание S2 требует закрытого UTC-дня, пересекающегося с нагрузкой.
S3 360/мин, 8 workers доступна только с явным `--allow-s3`; в приёмке #1510
этот флаг не используется. Заслонка сама возвращает S3 в S2 через максимум 6 ч.

Запуск под внешним supervisor (первый запуск или восстановление после ошибки):

```bash
docker exec espn-live-airflow-scheduler-1 python -m scrapers.espn.measure_pace run --benchmark-at-boundaries
```

`run` не удаляет `measurement.off`, не начинает завершённый замер, возвращает 0
при явном стопе/завершении и ненулевой код при ошибке. Второй процесс отклоняется
lock без изменения заслонки владельца. Только явные `start`/`resume` удаляют
стоп-файл этого замера. Первоначальный ручной запуск:

```bash
docker exec espn-live-airflow-scheduler-1 python -m scrapers.espn.measure_pace start --benchmark-at-boundaries
```

Статус и основной способ остановки:

```bash
docker exec espn-live-airflow-scheduler-1 python -m scrapers.espn.measure_pace status --format json
docker exec espn-live-airflow-scheduler-1 python -m scrapers.espn.measure_pace stop
```

После stop проверять status до `stopped`/`failed`/`complete` (промежуточный статус
`draining` ещё не подтверждает выход): уже начатые HTTP и
последовательные записи должны закончиться. Контроллер возвращает потолок к
последней полностью принятой ступени (изначально S0), сохраняя все hold/cooldown.
Не заменять этот протокол убийством host-процесса `docker exec`: оно не гарантирует
передачу сигнала Python внутри контейнера. Connect/read timeout — 5/20 с;
ожидания разрешения/повтора проверяют стоп каждые 0,25 с. Read timeout — предел
бездействия чтения, не общий wall-clock deadline; длительность текущей записи
Trino также не ограничена этим числом. Supervisor должен ждать подтверждённого
дренажа; `TimeoutStopSec=infinity` с контролем статуса не обрывает commit.

Конечный статус сохраняется как `terminal_status` перед `draining`: прерывание
финальной публикации после принятия S2 восстанавливает `complete` без новых HTTP
и повторного окна S2. Остаток outbox отмечается `final_publication_interrupted`
и допубликуется командой `publish`.

Перед выходом контроллер join-ит все SQL-потоки и публикует конечный срез outbox
до времени завершения HTTP. Новые попытки других процессов не продлевают этот срез;
число пакетов ограничено исходным числом dirty-строк. Ошибка/занятый flock оставляет
версии dirty и явные `publication.error`/`publication.pending`. Принятые ступени
сохраняются, но публикацию доказательств нужно завершить отдельно. После выхода
контроллера безопасный повтор (тот же runtime lock, только ops HTTP journal, без
HTTP ESPN, Bronze, сброса baseline или удаления stop-флага):

```bash
docker exec espn-live-airflow-scheduler-1 python -m scrapers.espn.measure_pace publish
docker exec espn-live-airflow-scheduler-1 python -m scrapers.espn.measure_pace status --format json
```

`publish` возвращает 1 при ошибке или остатке среза, 0 после подтверждения всех
версий среза. `publication_pending` — общий остаток по последней публикации;
результат последнего среза находится в `publication`. Завершённый `run` сам
повторно не запускает измерение или публикацию. Текущий SQL при остановке нужно
дождаться: потоки не daemon и не отделяются от владельца lock; wall-clock предел
запроса Trino здесь не вводится. При замене контейнера сохраняются SQLite/outbox,
ID/baseline/completed, а новое окно начинается после первого нового наблюдения.

Явное возобновление после принятого стопа:

```bash
docker exec espn-live-airflow-scheduler-1 python -m scrapers.espn.measure_pace resume --benchmark-at-boundaries
```

Прикладной пример выделенной host-службы (после приёмки кода, не из feature-tree):

```bash
systemd-run --unit=espn-1510-measure --property=Restart=on-failure --property=RestartSec=15s --property=TimeoutStopSec=infinity /usr/bin/docker exec espn-live-airflow-scheduler-1 python -m scrapers.espn.measure_pace run --benchmark-at-boundaries
```

Сначала выполнить модульный stop и дождаться завершения внутри контейнера,
затем останавливать host-службу. Не добавлять `ExecStop` с постоянным stop-флагом:
этот hook выполняется также перед автоматическим restart и остановит восстановление.
Сигнал host-процессу `docker exec` не гарантирует сигнал Python в контейнере. Автодоставка может заменить
контейнер; service restart выполнит `run` на новом установленном модуле, общий
lock/state сохраняется в томе, разрыв наблюдений сбросит окно.

Если окно отклонено с `incomplete_coverage`, контроллер после drain HTTP повторно
читает durable attempts. Завершённая попытка с неполными доказательствами
(`finish` записал HTTP duration) закрывает непригодное окно: его отчёт и attempt IDs
сохраняются в `rejected_window` атомарно с `restart_pending`. Новое окно той же
ступени начинается по следующему реальному known debt observation после drain.
Пока оно ожидается, новые измерительные HTTP не допускаются. Незавершённый
`begin` без duration, обычные complete HTTP ошибки и операторский stop не запускают
этот restart. Attempts не удаляются, baseline и completed сохраняются; для нового
окна снова обязательны полная длительность, нагрузка, свежесть и прежние пороги.

Локальный payload HTTP-попытки хранит необязательные `error_type` и `error_phase`
для ошибок отправки (`request`), чтения (`read`) и закрытия ответа (`close`).
Имена классов ограничены разрешённым списком; остальные записываются как
`OtherTransportError`. Сообщения исключений, URL и traceback не сохраняются
в этих полях. `DirectTransportError` передаёт их вместе с `attempt_id` в
`request_error`, чтобы ошибку можно было сопоставить с попыткой без сравнения времени.
Старые записи без этих полей остаются допустимыми. Диагностика не публикуется
в `espn_http_attempt_v1` и не меняет `complete`, таймауты или гейты приёмки.

`--benchmark-at-boundaries` запускает изолированный стенд после квалификации
каждой S0/S1/S2. Все сетевые workers сначала дренируются, следующие сетевые окна
начинаются после стенда. Успешный стенд каждой ступени сохраняется вместе со временем
замера и повторно не запускается при устаревании сетевого доказательства; пороги
возраста доказательства не ослабляются, при разрыве нужно новое полное окно. Первые 20 ID и точные content-addressed payloads
фиксируются в `benchmark-sample.json` и одинаковы на всех ступенях. Стенд создаёт
уникальную схему `iceberg.espn_pace_bench_<uuid>` с теми же четырьмя схемами таблиц
и partition_spec, вызывает штатный `write_tournament_batch` (три последовательных
повтора) и проверяет число строк. Удаляются только таблицы/схема своего запуска.
Ошибка стенда остаётся в событиях/отчёте, производственного fallback нет.

Отдельный стенд допустим только после остановки/завершения контроллера (тот же
lock) и после записи локальных Summary замером:

```bash
docker exec espn-live-airflow-scheduler-1 python -m scrapers.espn.measure_pace benchmark --benchmark-count 20
```

Полезная история сохраняет длительности каждой последовательной Bronze-пачки
отдельно. Пустая очередь означает «нет замера записи»; результат изолированного
стенда помечен отдельно. Массовая производственная запись подтверждается в #1511.

Зависимости runtime: установленный ESPN release; существующие Trino/S3/Iceberg
настройки scheduler для штатного writer; доступ на создание/удаление собственных
benchmark-схем и публикацию ops-журналов; requests, pyarrow, pandas, Trino/PyIceberg
из того же образа; доступ на запись в espn_live_state. HTTP_PROXY/HTTPS_PROXY/
ALL_PROXY должны отсутствовать. Все caller-процессы должны разрешать потолок >=S2;
отсутствующий ESPN_GATE_STEP_CEILING разрешает policy max, но реальная ступень
по-прежнему только подтверждённая общая. `--expected-ids-sha256` может дополнительно
проверять SHA256 компактного JSON-массива отсортированных ID (не SHA CSV-файла).

Пакет накладки утренней сводки готовится отдельно в
`/root/espn-deliveries/1510/summary/`: полная копия, diff, manifest SHA256,
проверки уникальности якорей, backup и rollback. Живой файл меняется только после
отдельного принятия конкретного SHA; изменение исходника другой сессией означает
пересборку и повторную проверку пакета.
