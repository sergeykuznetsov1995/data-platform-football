# REVIEW — data-platform-football

**Дата:** 2026-07-06
**Ветка:** `feat/top5-historical-backfill` (+ незакоммиченные правки в рабочем дереве)
**Коммит:** `baa5680` (в ходе ревью внешний бэкфилл-процесс закоммитил ранее незакоммиченные правки understat/transfermarkt — `4d0b393`, `baa5680`; их содержимое не изменилось, ссылки актуальны)
**Охват:** `scrapers/` (69 py), `dags/*.py` + `dags/utils` + `dags/scripts` (58 py), `dags/sql/**` (87 `.sql` + 16 `.sql.j2`), `scripts/`, `compose.yaml` / `configs/` / `docker/` / `Makefile`, лёгкий проход по `tests/`. Исключены сгенерированные артефакты (`transform/`, `logs/`, `data/`, `memory/`).

**Метод:** 6 параллельных read-only ревью-агентов по зонам → адверсарная верификация каждой находки Critical/High отдельным скептиком → синтез. Код не изменялся.

**Сводка:** Critical — 1, High — 4, Medium — 5, Low — 7.

---

## Статус исправлений (2026-07-06)

Все 17 пунктов закрыты в рабочем дереве ветки `feat/top5-historical-backfill` (не закоммичено).

| Пункт | Статус |
|---|---|
| C1 | ✅ `memswap_limit` 10G→16G + оба устаревших комментария обновлены |
| H1 | ✅ модульный `_season_to_short()` (зеркало whoscored, passthrough короткой формы), все 10 инлайн-копий заменены; +8 юнит-тестов |
| H2 | ✅ `season`/`league` в `_pre_check_bronze` идут через `_safe_predicate_value` |
| H3 | ✅ `gate_scrape` (ShortCircuitOperator) в `dag_ingest_fbref` по образцу sofascore/clubelo: скип при external trigger, обход `run_scrape=True`; двойные суточные прогоны (пункт «б») — вне объёма, принятый паттерн проекта |
| H4 | ✅ `player_id_canonical` → `player_id` в empty-фолбэке |
| M1 | ✅ `xref_cap` свёрнут MAX+GROUP BY (паттерн #814); golden-фикстура синхронизирована |
| M2 | ✅ `seen_ids`-проверка в `_validate_team_aliases_schema` + тест |
| M3 | ✅ silver: `{{ clubelo_in_scope_leagues }}` из `competitions.yaml in_scope`; gold: `fct_team_elo` берёт лиги из `silver.xref_team` (clubelo); TODO(E8b) закрыт |
| M4 | ✅ проверено на VM: bcrypt-хеши superset/openmetadata НЕ совпадают с плейсхолдерами — пароли ротированы, риска нет |
| M5 | ✅ `TRINO_OAUTH2_REFRESH_KEY` добавлен в `.env.example` |
| L1 | ✅ пример в docstring печатает `masked_url` |
| L2 | ✅ WARN с match_id при скипе пустого ростера |
| L3 | ✅ `execute_query(sql, params)` + `?`-параметры в `_existing_game_keys` |
| L4 | ✅ `dags/utils/ingest_helpers.py` (`load_result`, `league_slug`), 4 DAG-файла на импортах |
| L5 | ✅ `AND confidence <> 'orphan'` в CTE `xp` |
| L6 | ✅ оба `.bak.*` удалены |
| L7 | ✅ `git rm` configs/hdfs|hive|spark + spark_jobs; Makefile-цели init-hdfs/test-spark/shell-spark удалены. Остаточное легаси (вне охвата ревью): `init-storage`, `shell-namenode`, hive-проверка в `test-trino`, `urls` (HDFS/Spark UI) |

> Общая оценка: код зрелый. Почти каждая рискованная функция снабжена комментарием с номером issue про конкретный прошлый баг, паттерны экранирования SQL / skip-existing / completeness-guard выдержаны последовательно. Большинство находок — это места, где известный в проекте паттерн забыли применить в одной конкретной точке (SQL-инъекция, конвертация сезона, дедуп, memswap).

---

## Critical

### C1. `airflow-scheduler` не запустится: `memswap_limit` (10G) меньше `memory` (16G)
- **Файл:** `compose.yaml:364` (`memory: 16G`) и `compose.yaml:370` (`memswap_limit: 10G`)
- **Суть:** лимит памяти подняли с 8G до 16G (под 2 параллельных sofifa-пула), но соседний `memswap_limit` остался равен старому значению 10G. Docker требует `memswap_limit >= memory`.
- **Чем грозит:** `docker run --memory=16g --memory-swap=10g` даёт `Minimum memoryswap limit should be larger than memory limit` (воспроизведено живым запуском в песочнице). `docker compose config` эту ошибку **не** ловит — она возникает только при реальном создании контейнера. Значит `make up-lite` / `docker compose up` на этой ветке упадёт именно на `airflow-scheduler` — ядре оркестрации; без него не поднимется ни один ingest/transform DAG. Комментарий рядом (`compose.yaml:367-369`) вдобавок устарел — всё ещё говорит про «8G memory limit».
- **Фикс:** поднять `memswap_limit` минимум до 16G (или выше — под запас на своп), либо убрать директиву целиком: комментарий сам отмечает, что своп на хосте сейчас 0 и `memswap_limit` — no-op. Заодно обновить устаревший комментарий.

---

## High

### H1. SofaScore: конвертация сезона ломается на уже-короткой форме (`2526` → несуществующий `2627`), паттерн скопирован 10 раз
- **Файл:** `scrapers/sofascore/scraper.py:193` и идентично на строках `331, 456, 1020, 1160, 1246, 1574, 1896, 2157, 2358`
- **Суть:** каждый метод, которому нужна короткая форма сезона, содержит копию:
  ```python
  season_str = str(season)
  if len(season_str) == 4 and season_str.isdigit():
      season_short = f"{season_str[2:4]}{int(season_str[2:4]) + 1:02d}"
  else:
      season_short = season_str
  ```
  Код предполагает, что `season` всегда year-start (`2024` → `'2425'`). Но короткая форма документирована как валидный вход: docstring `read_player_ratings` (`scraper.py:1064-1066`: «any format read_schedule understands — 2526 / "2526" / 2025») и help CLI-флага `--season` (`dags/scripts/run_sofascore_scraper.py:1445`: «2526 for 25-26 short»). При `season="2526"`: `season_str[2:4]="26"` → `int("26")+1=27` → `season_short="2627"`.
- **Чем грозит:** ручной бэкфилл, вызванный ровно как задокументировано (`--season 2526`, форма, которую CLI сам рекламирует), даёт две разные поломки в зависимости от метода:
  - `read_schedule` / capture-путь: `season_short_to_label('2627')='26/27'`, фильтр событий по `'26/27'` не матчит реальные `'25/26'` → тихий no-op (0 строк, только WARN в логе).
  - методы с прямой записью партиции (`read_player_ratings`, `read_match_capture`, `read_shotmap`, `read_match_stats`): данные пишутся в партицию `season='2627'`; последующий нормальный (year-start) прогон пишет корректный `'2526'` через `replace_partitions=['league','season']`, но битую партицию `'2627'` никогда не чистит → фрагментация/дубли.
  - *Нюанс (верификация):* в одном из 10 мест (`_resolve_match_ids`, ~`scraper.py:456`) баг замаскирован fallback-фильтром `isin([season_short, season_str])` — там симптома нет. Остальные 9 уязвимы.
- **Фикс:** добавить один общий хелпер (зеркало `scrapers/whoscored/scraper.py:49` `_season_to_soccerdata_str`, где ровно этот баг уже починен с passthrough уже-короткой формы, docstring прямо описывает его), заменить им все 10 инлайн-копий.

### H2. SQL-инъекция: незаэкранированные `season`/`league` из параметров запуска DAG в `_pre_check_bronze`
- **Файл:** `dags/dag_e3_backfill.py:236-240` (валидация — `_read_params`, `:166-192`)
- **Суть:** `season` и `league` приходят из ручного «Trigger DAG w/ config», проверяются только на «непустая строка» (без regex/whitelist/паттерна в `Param`) и подставляются напрямую в f-string: `f"... WHERE season = '{season}' AND league = '{league}'"`, исполняется `cur.execute(sql)`. Это первая таска в графе (`start >> pre_check >> ...`), выполняется на каждом запуске, не мёртвый код.
- **Чем грозит:** тот же класс уязвимости в этом же файле закрыт экранированием в трёх других местах (`_safe_predicate_value` в `utils/e3_dq.py`, `_safe_silver_value` в `utils/silver_tasks.py`) — двойной стандарт внутри одного файла. Соединение — на весь каталог `iceberg` под сервис-аккаунтом `airflow` с правами записи в silver/gold. Stacked-queries в Trino невозможны, но `season`/`league` позволяют вырваться из литерала и дописать `UNION SELECT` → чтение произвольных схем каталога; эксфильтрация через сообщение исключения в логе таски (`except Exception as e: logger.warning(...)`), доступном той же группе операторов. Пивот от «право триггерить один бэкфил-DAG» к «read-доступ ко всему каталогу». Сегодня риск ограничен узкой доверенной группой операторов (Airflow на паузе, доступ по VPN), но дефект остаётся при масштабировании доступа.
- **Фикс:** прогнать `season`/`league` через `_safe_predicate_value` (как соседние функции этого же файла) перед подстановкой.

### H3. `dag_master_pipeline` гоняет FBref ежедневно вместо раз в неделю и создаёт второй прогон в сутки для всех источников
- **Файл:** `dags/dag_master_pipeline.py:258-268` (в связке с `dags/utils/config.py:91-98`)
- **Суть:** master расписан ежедневно (`'0 14 * * *'`) и в цикле по `INGESTION_DAGS` (8 DAG'ов) создаёт `TriggerDagRunOperator(execution_date='{{ ds }}', reset_dag_run=True)`. `TriggerDagRunOperator` вызывает `create_dagrun` напрямую и **не смотрит** на собственный cron дочернего DAG.
  - **(а) FBref:** его расписание — `'0 6 * * 1'` (только понедельник), и в `dag_ingest_fbref.py` **нет** гейта `external_trigger` (в отличие от `dag_ingest_sofascore.py`/`dag_ingest_clubelo.py`, где тяжёлая ветка скипается при внешнем триггере). Значит master запускает полный Cloudflare-Turnstile-обход (nodriver, ~15-25 мин, single CF bypass на прогон) **7 раз в неделю вместо 1**.
  - **(б) суточные источники:** master передаёт `execution_date='{{ ds }}'` = полночь UTC, что не совпадает с `logical_date` собственного cron-рана дочернего DAG (напр. `07:00`), поэтому создаётся **независимый второй DagRun** того же DAG в тот же календарный день (не «сброс утреннего», как можно подумать по `reset_dag_run` — этот флаг тут защищает лишь ретрай самого master). Итог: каждый источник скрейпится дважды в сутки.
- **Чем грозит:** (а) — главный риск: 7-кратный рост попыток CF-обхода на самом хрупком по анти-боту источнике → бан/деградация. (б) смягчается skip-existing/conditional-GET на большинстве суточных источников (лишний прогон качает мало данных), но не бесплатен: лишние browser/session-запуски (WhoScored/Selenium), нулевая-дельта нагрузка.
- **Фикс:** не включать в `INGESTION_DAGS` источники с собственным cron (или синхронизировать день недели для FBref), либо триггерить с уникальным `run_id`/датой и гейтировать по «уже отработал сегодня»; для FBref добавить `external_trigger`-гейт по образцу sofascore/clubelo. DAG сейчас не активен только из-за общей паузы Airflow — это внешняя мера, а не защита в коде (роадмап предполагает его включение).

### H4. Расхождение схемы: empty-фолбэк `fct_player_market_value` возвращает `player_id_canonical` вместо `player_id`
- **Файл:** `dags/sql/gold/fct_player_market_value_empty.sql:11` (пара к `dags/sql/gold/fct_player_market_value.sql:96`)
- **Суть:** боевой SQL отдаёт колонку `player_id`, фолбэк — `player_id_canonical`; остальные 6 колонок и типы совпадают 1:1. Комментарий фолбэка «Identical schema to fct_player_market_value.sql» ложен. Это единственное расхождение среди всех `*_empty.sql` (выборочно сверены `fct_transfer_empty`, `fct_player_salary_empty`, `fct_player_fifa_rating_empty`, `fct_match_officials_empty` — везде совпадает).
- **Чем грозит:** `run_gold_transform` (`dags/utils/gold_tasks.py:102-122`) переключается на фолбэк, если отсутствует **хотя бы одна** из `require_silver` таблиц (`fotmob_player_market_value_history` / `transfermarkt_market_value_history`) — напр. при недоступности одного источника или в новом окружении. Таблица unpartitioned (`partition_columns=None` в `dag_transform_fbref_gold.py:182-183`), поэтому `CREATE OR REPLACE TABLE AS` пересоздаёт схему целиком с новым именем колонки **без ошибки** на этапе CTAS. Поломка становится громкой сразу после: DQ-гейт того же прогона (`validate_gold_quality`, `gold_tasks.py:496-498, 1215-1223`) обращается к `player_id` и падает `COLUMN_NOT_FOUND`. Но в окне между коммитом CTAS и падением DAG таблица уже live с неверной колонкой, а до следующего исправного прогона gold-слой остаётся в сломанном состоянии для Superset/ad-hoc-запросов. Нарушает и декларацию самого файла, и конвенцию `CLAUDE.md` («Gold uses plain ids: player_id»).
- **Фикс:** переименовать колонку в `fct_player_market_value_empty.sql:11` с `player_id_canonical` на `player_id`.

---

## Medium

### M1. `xref_cap` в `fct_team_season_stats.sql.j2` не защищён от fan-out (в отличие от `xref_tm` рядом)
- **Файл:** `dags/sql/gold/fct_team_season_stats.sql.j2:161-170` (CTE `xref_cap`), ср. с исправленным `xref_tm` `:149-159`
- **Суть:** `xref_tm` явно свёрнут `MAX(source_id) ... GROUP BY canonical_id, league, season` с комментарием про issue #814/#712 («2 transfermarkt-алиаса на один canonical → 18 дублей»). `xref_cap` — та же топология (raw `club_name` из `bronze.capology_player_salaries`, разрешение через `_generic`-бакет без source-специфичных алиасов), но объявлен просто `SELECT DISTINCT` без дедупа по `(canonical_id, league, season)`.
- **Чем грозит:** если в одном `(league, season)` `club_name` встретится в двух написаниях одного клуба («Sheff Utd» / «Sheffield United» — ровно сценарий #814), `xcap` даст 2 строки на команду-сезон, и `LEFT JOIN cap_finance ... ON capf.cap_club_name = xcap.cap_club_name` (`:744-747`) размножит **всю** строку `fct_team_season_stats` для этой команды, задвоив все метрики сезона.
- **Фикс:** применить паттерн `xref_tm`: `SELECT canonical_id, MAX(source_id) AS cap_club_name, league, season ... GROUP BY canonical_id, league, season`.

### M2. `dim_team` не защищён от дубликата `canonical_id` в конфиге (в отличие от dim_referee/manager/venue)
- **Файл:** `dags/sql/gold/dim_team.sql.j2:27-31` (CTE `team_meta`) + `dags/utils/medallion_config.py:93-135` (`_validate_team_aliases_schema`)
- **Суть:** валидатор `team_aliases.yaml` не проверяет уникальность `canonical_id` (нет `seen_ids`), хотя сёстры-валидаторы `_validate_referee_aliases_schema` (`:177-182`), manager (`:226-231`), venue (`:274-279`) все раисят `MedallionConfigError: duplicate canonical_id`. `dim_team.sql.j2` джойнит сырые VALUES из YAML без предварительного дедупа.
- **Чем грозит:** случайный дубль `canonical_id` при копипасте одной из ~34 команд не будет пойман при загрузке конфига (в отличие от такой же ошибки в venue/referee/manager) и даст тихий fan-out — клуб получит 2+ строки в `gold.dim_team`, ломая задокументированный PK (`dim_team.sql.j2:19`).
- **Фикс:** добавить проверку `seen_ids` в `_validate_team_aliases_schema` (~3 строки, симметрично сёстрам).

### M3. Хардкод лиги `'ENG-Premier League'` в ClubElo-цепочке — сломается при мультилиговом бэкфилле
- **Файлы:** `dags/sql/gold/fct_team_elo.sql:49,54` и `dags/sql/silver/xref_team.sql.j2:186,198`
- **Суть:** оба фильтруют ClubElo строго по `league = 'ENG-Premier League'` без параметризации (в `xref_team.sql.j2` даже стоит `-- TODO(E8b): мультилига требует clubelo↔league маппинг`). Это осознанное APL-only состояние, но именно текущая ветка (`feat/top5-historical-backfill`) ведёт мультилиговый бэкфилл.
- **Чем грозит:** когда ClubElo-скрейпер начнёт поставлять Bundesliga/La Liga/Serie A/Ligue 1, `xref_team` не сможет резолвить их имена → строки новых лиг станут orphan или выпадут из universe, а `gold.fct_team_elo` для них будет молча пустым (фильтр в WHERE, не JOIN — ошибки нет, просто 0 строк).
- **Фикс:** вынести список лиг в конфиг (`configs/medallion/competitions.yaml` / `dim_competition`), синхронно в обоих местах — по образцу `fct_standings.sql`, где уже `WHERE season IN (SELECT season FROM dim_season)`.

### M4. Плейсхолдер-пароли Trino service-аккаунтов раскрыты в закоммиченном `.env.example`
- **Файл:** `.env.example:56-65`
- **Суть:** комментарий прямо называет пароли (`superset_trino_pass_2026`, `om_trino_pass_2026`), из которых генерировались bcrypt-хеши в `configs/trino/password.db` для аккаунтов `superset`/`openmetadata`. Сам `password.db` в git **не** отслеживается (проверено `git ls-files` / `git log --diff-filter=A` — пусто, защищён `.gitignore`), так что активной утечки хешей нет.
- **Чем грозит:** если на проде реально используются эти плейсхолдер-пароли (а не сгенерированы новые через `htpasswd`), любой читающий репозиторий залогинится в Trino под сервисным аккаунтом. Авторы это осознают — комментарий помечен «rotate-me-before-prod».
- **Фикс:** подтвердить, что на VM пароли ротированы (новый `htpasswd -B`, пересборка `password.db`). В коде менять нечего — предупреждение уже честное. (См. «На обсуждение».)

### M5. Обязательная переменная `TRINO_OAUTH2_REFRESH_KEY` отсутствует в `.env.example`
- **Файл:** `configs/trino/config.properties:37` (`...refresh-tokens.secret-key=${ENV:TRINO_OAUTH2_REFRESH_KEY}`), потребитель `compose.yaml:434`; в `.env.example` записи нет (соседний `TRINO_OIDC_CLIENT_SECRET` — есть, `:81`).
- **Чем грозит:** развёртывание «с нуля» по `.env.example` (единственный документированный bootstrap) оставит переменную пустой → Trino получит пустой secret-key для refresh-токенов OAuth2: либо не стартует, либо тихо сломает refresh-токены (ради которых патч и делался — иначе рестарт Trino разлогинивает всех аналитиков).
- **Фикс:** добавить в `.env.example` строку `TRINO_OAUTH2_REFRESH_KEY=<openssl-rand-base64-32>` рядом с `TRINO_OIDC_CLIENT_SECRET`.

---

## Low

### L1. Пример в docstring DAG печатает proxy-URL с кредами в открытом виде
- **Файл:** `dags/dag_ingest_fbref.py:291`
- **Суть:** `print(f'First proxy URL: {pm.get_http_proxy_url()}')` в docstring-разделе «Testing proxy connectivity»; `get_http_proxy_url()` (`scrapers/utils/proxy_manager.py:605-619`) возвращает `http://user:pass@host:port`. Это не исполняемый код, но docstring рендерится в Airflow UI, и оператор, скопировавший команду, залогирует полные креды прокси. Во всех реальных вызовах прокси в коде маскируется (`nodriver_fbref/scraper.py:211`, `sofascore/scraper.py:784`, `fbref/browser_manager.py:322`).
- **Фикс:** заменить пример на маскированный вывод (`.masked_url` / срез после `@`).

### L2. Understat: пустой ростер матча тихо пропускается без логирования и счётчика
- **Файл:** `scrapers/understat/scraper.py:64-68`
- **Суть:** при пустом ростере матч пропускается (`return None`) без лога/метрики — в отличие от остальных failure-путей (`ConnectionError`, `KeyError`). Нет видимости, сколько матчей так потерялось за прогон. Не критично (единичный матч на xG-таблицах).
- **Фикс:** логировать пропуск на WARN + счётчик пропущенных матчей.

### L3. ESPN: SQL строится ручным экранированием кавычек вместо параметризации
- **Файл:** `scrapers/espn/scraper.py:437-450` (`_existing_game_keys`)
- **Суть:** запрос строит `replace("'", "''")` вместо параметризованного `?`. `league`/`season` приходят из внутреннего конфига DAG, не от пользователя — реальной инъекции сейчас нет, но паттерн хрупкий, если `league`/`season` когда-то станут user-facing.
- **Фикс:** перейти на параметризованный запрос (как в `run_sofascore_scraper.py`).

### L4. Дублирование хелперов между DAG-файлами
- **Файлы:** `dags/dag_ingest_capology.py:54`, `dag_ingest_sofascore.py:76`, `dag_ingest_transfermarkt.py:57` (идентичная `_load_result`); `dag_ingest_whoscored.py:55`, `dag_ingest_capology.py:49` (идентичная `_league_slug`)
- **Суть:** одна реализация скопирована в 2-3 файла вместо выноса в `utils/`. Правка в одной копии (напр. обработка кодировки) не попадёт в остальные.
- **Фикс:** вынести в `dags/utils/` (рядом с `bronze_validation.py`).

### L5. Непоследовательный xref-фильтр в `whoscored_player_season_aggregate`
- **Файл:** `dags/sql/silver/whoscored_player_season_aggregate.sql:51-55`
- **Суть:** CTE `xp` берёт `xref_player WHERE source='whoscored'` без `AND confidence <> 'orphan'` — в отличие от симметричных `understat_player_season_aggregate.sql:27-32` и `sofascore_player_season_aggregate.sql:29-34`. В таблице остаются orphan-строки с `canonical_id='ws_<id>'`. Сегодняшний единственный потребитель джойнит по FBref-spine (`fb_*`), поэтому orphan просто не матчатся и искажения нет; риск — будущий консьюмер, обратившийся к таблице напрямую.
- **Фикс:** добавить `AND confidence <> 'orphan'` в CTE `xp` для консистентности.

### L6. Бэкап-файлы конфигов Trino оставлены в рабочем дереве
- **Файлы:** `configs/trino/config.properties.bak.1783081732`, `configs/trino/jvm.config.bak.1783081732` (untracked)
- **Суть:** точные копии предыдущих версий конфигов (автобэкап перед правкой). Реальных секретов внутри нет (только `${ENV:...}`), но `git add configs/trino/` захватит дубли, и при следующем ревью неясно, какой файл актуален.
- **Фикс:** удалить оба `.bak.*`; историю вести через git.

### L7. Легаси-конфиги и Makefile-цели мёртвых сервисов (HDFS/Hive/Spark)
- **Файлы:** `configs/hdfs`, `configs/hive`, `configs/spark`, `spark_jobs/`, цели `Makefile` (`init-hdfs`, `test-spark`, `shell-spark`, справка `make up`)
- **Суть:** платформа мигрировала HDFS→SeaweedFS и Hive/Spark→Lakekeeper/Trino; в `compose.yaml` нет ни одного hdfs/hive/spark-сервиса. Подтверждено как известный техдолг в `CLAUDE.md`.
- **Фикс:** удалить директории/цели или пометить `# LEGACY` явно. Отмечено для полноты — уже задокументировано.

---

## На обсуждение

- **M4 — ротация паролей на проде (не проверяется из репо):** активной утечки нет (хеши `password.db` не в git), но подтвердить факт ротации плейсхолдер-паролей на живой VM отсюда невозможно — это операционная проверка на стороне владельца. Если пароли ротированы — находку можно закрыть; если нет — это фактически Critical (вход под сервис-аккаунтом Trino по паролю из публичного `.env.example`).
- **H2/H3 — модель угроз и режим Airflow:** сейчас Airflow на VM в ручном режиме на паузе, доступ операторов узкий и через VPN (Headscale). Это снижает сегодняшнюю эксплуатируемость обеих находок, но обе — про код «как есть» на случай включения по роадмапу и масштабирования доступа операторов (issue про 100 пользователей / RBAC). Нужно решить: чинить сейчас или занести в бэклог с явной привязкой к моменту включения `dag_master_pipeline` / расширения RBAC.
- **H1 — намеренная ли APL-only мультилига:** `UNDERSTAT_LEAGUES` и подобные конфиги пока = только АПЛ, несмотря на комментарии «top-5». Похоже на намеренный промежуточный шаг постепенного включения лиг, а не баг — но стоит подтвердить, что короткая форма сезона (`--season 2526`) реально используется в бэкфилл-скриптах, иначе приоритет H1 можно понизить.
- **M3 vs авторский TODO:** хардкод лиги в ClubElo уже помечен `TODO(E8b)` — вопрос лишь в тайминге: успеет ли параметризация к моменту, когда ClubElo начнёт поставлять не-APL данные на этой ветке. Если ClubElo для новых лиг ещё не включён — риск отложенный.
- **Область ревью:** `.sql.j2`-шаблоны и полный (не diff) проход по DAG изначально едва не выпали из охвата — если в будущем ревьюить только diff ветки, эти находки (H2, H3, H4, M1, M2) не всплывут, т.к. затронутые файлы не менялись на ветке. Рекомендация: ревьюить состояние кода, а не только diff.

---

## Покрытие

| Зона | Глубина |
|---|---|
| `scrapers/` (base/utils/schemas + fbref/whoscored/sofascore) | полное построчное по ядру и 3 сложным; остальные `.py` — грепом по антипаттернам |
| `scrapers/` (transfermarkt, fotmob, espn, understat, matchhistory, capology, clubelo, sofifa) | полное построчное |
| `dags/*.py`, `dags/utils`, `dags/scripts` (58 файлов) | полное построчное по ingest/master/e3 + греп-проход по transform-DAG |
| `dags/sql/**` — 87 `.sql` + 16 `.sql.j2` | все 103 файла прочитаны полностью |
| `compose.yaml`, `configs/`, `docker/`, `Makefile`, `scripts/` | ключевые — полностью; `scripts/*.py` — грепом по секретам/`set -e` |
| security-sweep (репо-wide) | секреты (git ls-files / log --diff-filter=A), инъекции, TLS, логи, path traversal |
| `tests/` (208 файлов) | лёгкий греп-проход (skip без причины, дубли, тесты без assert) — подозрительного не найдено |

**Не покрыто / вне зоны:** живые логи Airflow/Trino на VM (ревью read-only по репозиторию, не по runtime); статические сканеры bandit/semgrep не прогонялись (только целевой grep); часть anti-bot browser-модулей `scrapers/base/browser/*` прочитана выборочно, не построчно.
