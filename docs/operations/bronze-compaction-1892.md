# Bronze compaction #1892

Локальные код и проверки не доказывают доставку или production-приёмку.
Разовый запуск на общем стеке выполняет лид после отдельного разрешения владельца.

Ночной профиль: 03:00–05:00 UTC (06:00–08:00 МСК; перенос по решению владельца
08.11; граница — `dags/utils/maintenance_window.py`), один запрос одновременно, ≤1024 файлов / 512 МиБ
на порцию, ≤120 порций / 8 ГиБ за ночь. Каждая порция перечитывает `$files`;
очередь таблиц и deadline сохраняются в Airflow Variable на DagRun/task.
Общая Variable `bronze_compaction_shared_budget` хранит бюджет UTC-ночи для daily
и всех catch-up запусков и позиции партиций между ночами. До выполнения порции
бюджет резервируется транзакцией metadata DB под PostgreSQL advisory lock;
неудачная порция тоже расходует резерв. В новую ночь обнуляется только бюджет.
Один poke исполняет максимум одну порцию; reschedule через 5 с освобождает pool.
Занятый publication lock FBref возвращает таблицу в очередь: проход продолжается по
другим источникам; в direct пауза 60 с делается, только когда вся оставшаяся очередь
вернула busy.
FBref получает отдельный publication generation на порцию, renew/assert перед SQL
и во время запроса. PostgreSQL connect/statement/lock ограничены; отмена Trino
работает независимо от heartbeat. Утрата ownership прекращает проход.

Сохраняются `COMPACTION_DELETE_FILE_POLICY`, точный WHERE по `$path`, SQL/Python
потолки и ошибки настоящих таблиц. WhoScored требует production admission;
runtime evidence обновляется штатным генератором. FotMob использует физический
контракт: строки, `_target_batch_id`, manifest и current views не меняются.
Transfermarkt не добавлен в новую allowlist; существующие writer lock, commit retry
и snapshot anchors остаются в общем maintenance. Weekly сохраняет Silver/Gold
компакцию и retention 30d; Bronze компакция перенесена в ночь. Daily retention остаётся
WhoScored 14d / прочие 3d и не расширяется вместе с целями компакции.

## Расчёт и ограничения

Снимок 28.10.2026: 60 таблиц, 758569 файлов, 33,05 ГиБ. Максимум профиля —
122880 выбранных файлов / ночь: нижняя оценка погашения долга — 7 окон.
Это не прогноз: из одного снимка нельзя получить суточный прирост или CPU/скорость
порции. До приёмки нужны измерения additions и скорости. Партиции, delete files,
новые поступления и занятое окно могут увеличить срок. ClubElo historical ждёт
решения владельца; старая дата commit не считается доказательством заморозки.

Прогресс подтверждается исчезновением выбранных путей и уменьшением числа data-файлов.
Параллельные additions могут скрыть уменьшение общего числа: тогда цель
консервативно получает ошибку и снимается с очереди до следующего запуска.
Skipped/delete/pending/not_present цели видны в результате; ошибки не дают зелёный DAG.
Variable с префиксом `bronze_compaction_` сохраняет checkpoint и итог для аудита;
удалять завершённые checkpoints только штатной уборкой metadata.
Общую `bronze_compaction_shared_budget` не удалять: она сохраняет бюджет текущей
ночи и продвижение партиций между ночами. Пул FBref должен иметь
один слот. Catch-up и daily сериализует этот пул; другие ручные OPTIMIZE требуют
исключить перекрытие по протоколу общего стека.

## Догоняющий запуск

В доставленном maintenance runtime с Airflow CLI и доступом к его metadata DB:

```bash
python scripts/compact_bronze.py --table fbref_schedule --table fbref_match_managers \
  --seconds 1800 --portions 20 --bytes 1073741824 \
  --journal /shared/maintenance/1892-run1.jsonl --stop-file /shared/maintenance/1892-run1.stop
```

По умолчанию dry-run. CLI ставит `dag_iceberg_compaction_catchup` в очередь;
код возврата CLI подтверждает постановку, не выполнение. Проверить task state и JSONL.
Для записи повторить с `--apply` после разрешения владельца. Пути журнала/стопа
должны быть доступны worker и лиду на общем mount; использовать уникальные на запуск.
Остановка: та же CLI с `--stop --stop-file /shared/maintenance/1892-run1.stop`.
Marker проверяется до SQL и во время запроса; deadline включает ожидание в очереди,
reschedule и выполнение. Повторный запуск создаёт новый run и перечитывает metadata.
Retention CLI не запускает.

После доставки лид подтверждает fbref_schedule ≤200 файлов, дневной EXPLAIN ANALYZE
≤10 с, следующую ночь и доступность утренней сводки. Этого локально не проверяли.

## Инвентаризация

Источник: переданный `bronze-snapshot-summary-20261028.csv`; число data-файлов
последнего snapshot (не повторный live `$files` аудит). Критерий: >1000 и средний <1 МиБ.
Старый список не доказывает выполнение компакции: daily раньше включал её только
для WhoScored. Новая allowlist сверена с typed/generic FBref, ESPN native, SofaScore
и физическим контрактом FotMob. Таблица предназначена для комментария к issue.

| Таблица | Файлы | Средний, байт | Старый HIGH_CHURN | Решение |
| --- | ---: | ---: | --- | --- |
| clubelo_ratings_historical | 1609 | 4990 | нет | ждёт решения владельца |
| espn_match | 3065 | 10921 | нет | ночная компакция |
| espn_match_events | 1870 | 22963 | нет | ночная компакция |
| espn_match_lineup | 1687 | 16984 | нет | ночная компакция |
| espn_team_stats | 1515 | 10355 | нет | ночная компакция |
| fbref_dataset_availability | 16995 | 2912 | нет | ночная компакция |
| fbref_keeper_keeper | 2295 | 9260 | нет | ночная компакция |
| fbref_lineups | 16283 | 4841 | да | ночная компакция |
| fbref_match_events | 16992 | 4853 | да | ночная компакция |
| fbref_match_keeper_stats | 16156 | 4159 | нет | ночная компакция |
| fbref_match_managers | 17979 | 2676 | нет | ночная компакция |
| fbref_match_officials | 16901 | 2937 | нет | ночная компакция |
| fbref_match_player_stats | 16660 | 8746 | да | ночная компакция |
| fbref_match_team_stats | 17981 | 4258 | да | ночная компакция |
| fbref_page_manifest | 26322 | 3062 | нет | ночная компакция |
| fbref_player_misc | 2297 | 19471 | нет | ночная компакция |
| fbref_player_playingtime | 2276 | 33629 | нет | ночная компакция |
| fbref_player_shooting | 1730 | 22708 | нет | ночная компакция |
| fbref_player_stats | 2290 | 25589 | нет | ночная компакция |
| fbref_schedule | 15244 | 10092 | нет | ночная компакция |
| fbref_table_cells | 25492 | 379524 | нет | ночная компакция |
| fbref_table_inventory | 26130 | 5836 | нет | ночная компакция |
| fbref_team_misc | 2305 | 5925 | нет | ночная компакция |
| fbref_team_playingtime | 2275 | 7740 | нет | ночная компакция |
| fbref_team_shooting | 1730 | 6624 | нет | ночная компакция |
| fbref_team_stats | 6315 | 8006 | нет | ночная компакция |
| fotmob_competition_scope_observations | 221849 | 7006 | да | ночная компакция |
| fotmob_competition_season_history | 19456 | 6938 | да | ночная компакция |
| fotmob_competitions | 5062 | 28742 | да | ночная компакция |
| fotmob_leaderboard_categories | 10731 | 6103 | да | ночная компакция |
| fotmob_leaderboards | 9248 | 32069 | да | ночная компакция |
| fotmob_match_payloads | 22290 | 269000 | да | ночная компакция |
| fotmob_matches | 14087 | 14673 | да | ночная компакция |
| fotmob_player_snapshots | 8447 | 305199 | да | ночная компакция |
| fotmob_playoff_brackets | 2512 | 8241 | да | ночная компакция |
| fotmob_season_stages | 5415 | 7317 | да | ночная компакция |
| fotmob_season_teams | 14090 | 7495 | да | ночная компакция |
| fotmob_standings | 10804 | 9697 | да | ночная компакция |
| fotmob_transfer_events | 2805 | 34246 | да | ночная компакция |
| sofascore_event_participants | 6711 | 41630 | нет | ночная компакция |
| sofascore_event_player_stats | 3416 | 83122 | да | ночная компакция |
| sofascore_event_shotmap | 2177 | 98311 | нет | ночная компакция |
| sofascore_events | 6711 | 116117 | нет | ночная компакция |
| sofascore_incidents | 6408 | 92623 | нет | ночная компакция |
| sofascore_league_table | 4239 | 6952 | нет | ночная компакция |
| sofascore_lineups | 3342 | 24479 | нет | ночная компакция |
| sofascore_match_capture_status | 6973 | 2922 | нет | ночная компакция |
| sofascore_match_stats | 4580 | 57166 | да | ночная компакция |
| sofascore_player_ratings | 3466 | 26831 | да | ночная компакция |
| sofascore_schedule | 63020 | 80712 | нет | ночная компакция |
| sofascore_venue | 2357 | 13121 | нет | ночная компакция |
| whoscored_match_bets | 1917 | 14049 | да | ночная компакция |
| whoscored_match_incidents | 3511 | 19846 | да | ночная компакция |
| whoscored_match_ingest_manifest | 9405 | 7447 | да | ночная компакция |
| whoscored_player_profile_versions | 2390 | 7037 | да | ночная компакция |
| whoscored_player_stage_participations | 1981 | 13363 | да | ночная компакция |
| whoscored_player_stage_stats | 4775 | 1001801 | да | ночная компакция |
| whoscored_profile_ingest_manifest | 2363 | 5608 | да | ночная компакция |
| whoscored_schedule | 4824 | 43494 | да | ночная компакция |
| whoscored_scope_ingest_manifest | 4813 | 10485 | да | ночная компакция |

## Прямой разовый запуск с хоста

До следующего общего выпуска лид использует `--direct`: Airflow не импортируется,
DAG не запускается. Cron устанавливает лид. Окно и ограничения те же; без `--seconds`
один вызов работает до 05:00 UTC (08:00 МСК), бюджета, stop или исчерпания очереди. Airflow-режим
без `--direct` по-прежнему имеет default 1800 с и admission через pool.

```bash
/root/.venvs/dpf-test/bin/python scripts/compact_bronze.py --direct \
  --table fbref_schedule --table fbref_match_managers \
  --state-file /shared/maintenance/1892-direct-state.json \
  --journal /shared/maintenance/1892-direct.jsonl \
  --stop-file /shared/maintenance/1892-direct.stop
```

По умолчанию dry-run; `--apply` включает запись. CLI сам не читает `.env`.
Обёртка лида передаёт `TRINO_USER`, `TRINO_PASSWORD`, `FBREF_CONTROL_DB_URI` через
окружение. Trino: HTTPS `127.0.0.1:8082`, self-signed (`verify=False`). URI control
должен указывать на хостовый PostgreSQL `127.0.0.1:5432/airflow`; он же используется
для read-only проверки `public.dag_run`, connect timeout 2 с / statement timeout 5 с.
FBref использует прежний publication lock с renew/assert и отменой Trino.

Перед первой и каждой следующей порцией direct ждёт отсутствия running
`dag_iceberg_maintenance_daily` / `dag_iceberg_maintenance`. Если daily с
`execution_date` предыдущего дня 03:00 UTC (logical date ночного запуска по
расписанию `0 3 * * *`) существует, он должен быть завершён (success/failed).
Пока прод-дерево держит старое расписание `0 2 * * *`, его запуск начинается до
окна; running daily в любом случае блокирует direct. Если ожидание не закончилось
до 05:00 UTC, ночь пропускается (exit 0, `deadline` в журнале). Ошибка БД запрещает
новую порцию и завершает CLI с exit 1. Weekly в воскресенье 05:00 находится за
пределами окна. Проверка не является общей блокировкой: ручной запуск старого DAG
после SELECT исключает лид по протоколу общего стека.

Занятый publication lock FBref не останавливает прямой вызов: таблица переставляется
в конец очереди state без изменения бюджета и курсоров, и следующая порция берётся у
другого источника без ожидания. Пауза 60 с (в пределах deadline) — только когда вся
оставшаяся очередь вернула busy, после чего набор busy сбрасывается и попытка
повторяется до deadline. Свободный FBref не гарантируется.

### Приоритет direct-очереди

Repeatable `--priority-table` работает только с `--direct`: приоритетные таблицы
идут раньше остальных, а round-robin внутри приоритетной и обычной групп сохраняется.
Все значения должны быть уникальными и входить в полный набор `--table`. Busy-таблица
временно остаётся в конце текущего прохода и не останавливает остальные.

```bash
/root/.venvs/dpf-test/bin/python scripts/compact_bronze.py --direct \
  --table fbref_schedule --table fbref_match_managers \
  --table fotmob_competition_scope_observations \
  --priority-table fbref_schedule \
  --priority-table fotmob_competition_scope_observations \
  --state-file /shared/maintenance/1892-direct-state.json \
  --journal /shared/maintenance/1892-direct.jsonl \
  --stop-file /shared/maintenance/1892-direct.stop
```

Приоритет сохраняется в общем state и используется при продолжении без повторной
передачи параметра, в том числе на следующую ночь. Новый explicit набор заменяет
сохранённый; выпавшие из allowlist таблицы удаляются из приоритета. Бюджет и
partition cursors не сбрасываются. Старый state без приоритета сохраняет прежний
round-robin. Не сокращайте `--table` до подмножества и не создавайте второй
state-file. Окно, лимиты и остальные guards не меняются.

Все прямые apply-запуски используют **один state-file и одинаковый набор таблиц**.
Не удалять state и стабильный соседний `.lock`: lock держится весь процесс,
JSON заменяется атомарно с fsync. Перед ALTER сохраняются расход ночного бюджета,
позиция партиции и ротация таблицы; аварийная/неудачная попытка расходует резерв.
Следующей UTC-ночью обнуляются только счётчики; позиции и незаконченная очередь
сохраняются, завершённая очередь снова заполняется. Deadline и `$path` не сохраняются.
Повреждённый/несовместимый state означает отказ; не обнулять его вручную.
Изменение allowlist между ночами (новая или замороженная таблица) — не отказ:
бюджет и позиции уцелевших таблиц сохраняются, новые таблицы встают в конец
ротации, замороженные выпадают из очереди вместе с курсором (ночь 04.11: две новые
таблицы WhoScored остановили склейку с `ValueError`).
Dry-run не меняет боевой state, не расходует бюджет и не выполняет ALTER.

JSONL содержит числовые события порций (в том числе files_before/files_after)
и итог: порции/байты текущего вызова, файлы по таблицам, причина и exit code.
Ошибки записываются только как тип/причина, без текста исключений и секретов.
Stop-marker действует до и во время SQL; штатная остановка/deadline/budget — exit 0,
ошибка состояния, lock, БД, fencing или порции — exit 1. Убитый процесс может
не успеть записать итог, но уже сохранённый резерв остаётся в state.

**Перед доставкой Airflow-исполнителя лид останавливает direct через stop-файл и
снимает cron.** Бюджеты Airflow и локального файла независимы; одновременный запуск
недопустим. Первый боевой запуск лид показывает владельцу после dry-run согласно
решению по #1892; эта документация не доказывает production-приёмку.

## Обслуживание схемы ops (журналы источников)

Таблицы `iceberg.ops.*` (журналы попыток, сигналы, манифесты) пишутся MERGE каждые
30–60 с круглосуточно и копят мелкие data/delete-файлы и снапшоты. Склейка Bronze их
не берёт: без партиций и с живыми delete-файлами таблица пропускается целиком.
Поэтому для ops — отдельный проход `dags/utils/ops_maintenance.py::maintain_ops_tables`:

1. список BASE TABLE схемы из `information_schema`, порядок — по убыванию числа
   файлов в `$files`;
2. по каждой таблице — полный `ALTER TABLE … EXECUTE optimize` (без `$path`,
   применяет delete-файлы), затем `expire_snapshots(retention_threshold => '3d')`
   (сессия выставляет `iceberg.expire_snapshots_min_retention = '1h'`).
   Expire идёт порциями не более `EXPIRE_CHUNK` (5000) снапшотов за коммит, от
   старых к новым: Iceberg шлёт по одному `remove-snapshots` на снапшот, а
   Lakekeeper 0.13 отвечает на тело запроса больше ~2 МБ текстовым 413 (ночь
   04.11: `ops.espn_http_attempt_v1`, 33,8 тыс. снапшотов к удалению);
3. конфликт коммита с параллельным MERGE (`ICEBERG_COMMIT_ERROR`) — до 4 попыток с
   backoff; если не вышло — таблица `busy`, проход идёт дальше;
4. каждый ALTER ограничен `query_max_run_time` до deadline; deadline и stop-файл
   проверяются между таблицами.

`remove_orphan_files` на ops **не выполняется**: активные писатели могут владеть
файлами, ещё не попавшими в закоммиченные метаданные (тот же контракт, что у
`maintain_sofascore_capture_manifest`). Отдельная weekly-задача
`maintain_sofascore_capture_manifest` из `dag_iceberg_maintenance` убрана — манифест
теперь обслуживается ежедневно вместе с остальными ops-таблицами.

В Airflow это задача `maintain_ops` в `dag_iceberg_maintenance_daily` (после
`maintain_other_high_churn_bronze`, `execution_timeout` 1 ч). `busy`-таблицы задачу
не валят; падает она при ошибке не-конфликтного типа или если все таблицы `busy`.

### Прямой запуск с хоста

До доставки DAG в общий планировщик:

```bash
/root/.venvs/dpf-test/bin/python scripts/maintain_ops.py --direct \
  --journal /shared/maintenance/ops-direct.jsonl \
  --stop-file /shared/maintenance/ops-direct.stop \
  [--apply] [--seconds 3600] [--table espn_http_attempt_v1 ...]
```

Окно — то же 03:00–05:00 UTC (06:00–08:00 МСК); вне окна CLI завершается ошибкой
аргументов (exit 2). По умолчанию dry-run: только метрики и план, без ALTER.
Окружение: `TRINO_USER`, `TRINO_PASSWORD`, `FBREF_CONTROL_DB_URI` (CLI сам `.env` не
читает); Trino `https://127.0.0.1:8082` (`verify=False`); перед стартом read-only
проверка `public.dag_run`, как у direct-склейки: при running maintenance-DAG —
exit 1 без ALTER. Запускать **последовательно** перед `compact_bronze.py --direct`
(`maintain_ops.py … && compact_bronze.py …`), не параллельно.

Журнал JSONL: событие `table` на таблицу (`before`/`after` = `{files, snapshots}`,
`status` ok/busy/failed/planned/deadline) и итог `summary` (`maintained`, `busy`,
`failed`, `stop_reason`, `exit_code`). Тексты исключений и секреты не пишутся —
только тип ошибки. Остановка: `touch <stop-file>` — проход закончится после текущей
таблицы. Exit 0 — хотя бы одна таблица обслужена или штатная остановка; exit 1 —
ошибка или все таблицы `busy`.
