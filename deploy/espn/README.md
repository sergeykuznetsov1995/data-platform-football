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
  одном дне → один запрос `competitions/{id}/status`: 404 → `withdrawn`, ответ есть → `moved`
  (kickoff прежний: статус даты не несёт; новый статус из ответа пишется в строку, финал — с
  summary, см. ниже). Помеченный матч повторно не проверяется каждой волной. Больше
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
  файл добавлен `git add -f` — `*.json` в `.gitignore`). `airflow-init` делает только
  `airflow db migrate` + `airflow pools import`; запускается при посеве и когда `pools.json`
  цели отличается от живого.

### Автомат `auto_deliver.py`

Хост-копия `/root/espn-deploy/auto_deliver.py` (= master, иначе «обнови автомат» в Telegram и
стоп), cron `*/5`, лог — `/root/watchdog/espn_auto_deliver_cron.log`. Каталог проекта compose —
`/root/espn-deploy` (метка `working_dir` не держит дерево кода). Состояние
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
running/queued `dag_espn_current` и до волны 00/06/12/18 UTC ≥ 10 мин → корень релиза →
[`run --rm --no-deps -T airflow-init`] → `up -d --no-deps --force-recreate airflow-scheduler
airflow-webserver` с `ESPN_RELEASE_ROOT=<корень>` в окружении вызова → приёмка ≤ 7 мин:

- `dag.last_parsed_time` `dag_espn_current` позже момента выката, `has_import_errors = f`,
  `import_error` = 0 (свежая ошибка разбора — провал сразу);
- `/opt/airflow/dags` scheduler смонтирован из корня цели; sha256 файлов `deploy/espn/dags` и
  `scrapers/espn` в контейнере = корню;
- пулы metadb = `pools.json` (слоты каждого объявленного пула);
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
