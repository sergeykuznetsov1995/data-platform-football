# Сохранённые доказательства актуалки (#1358/#1359)

`scrapers/sofascore/refresh_evidence.py` и CLI
`scripts/sofascore_refresh_evidence.py` работают только с локальными JSON.
Они не обращаются к SofaScore, Airflow или Trino, не назначают t0 и не снимают hold.
Старые восемь выбранных планов из одиннадцати запусков не являются полным
знаменателем спроса. Их повторная обработка остаётся UNKNOWN.

## Сохранение существующего чтения

`planning_snapshot(identity, partitions, selected, core_ids=..., daily_ids=...,
excluded_ids=..., disputed_ids=..., input_complete=True, observed_at=...)`
принимает **все** строки существующего `PENDING_PARTITIONS_SQL` до ограничения
числа скоупов. Формат строки: `[league, canonical_season, pending_matches,
nearest_open_deadline_epoch, open_matches, native_season_id]`. Каждый родной
турнир/сезон сохраняется один раз. Отдельно сохраняются выбранные скоупы,
их `attempt_id`, фаза open/debt и полное окружение с лимитами. Daily, excluded,
disputed и неизвестные разделяются; неизвестная классификация блокирует PASS.
Например, 21 должный скоуп и 8 выбранных дают знаменатель 21, а не 8.

`identity` содержит `run_id`, полный 40-символьный `code_revision`,
`artifact_id`, `denominator_sha256`, `snapshot_id`, `campaign_id` (SHA256),
`logical_date` и фактический `started_at` с часовыми поясами. `observed_at` — время
исходного чтения. Самодекларированное `input_complete` и digest не доказывают
полноту: при передаче дежурный сверяет происхождение с сохранённым чтением и
полным перечнем ожидаемых запусков. Digest защищает от случайного изменения,
а не заменяет содержательную проверку.

CLI с `--capture` принимает JSON аргументов `planning_snapshot` и сохраняет его:

```bash
PYTHONDONTWRITEBYTECODE=1 python scripts/sofascore_refresh_evidence.py \
  --capture --input saved-planner-read.json --output immutable-plan.json
```

`write_immutable` публикует JSON через fsync и атомарный no-overwrite link.
Точный повтор разрешён; другая запись по тому же пути отклоняется.

## Проверка сохранённого окна

Без `--capture` вход CLI — `{expected_revision: <full SHA>, expected_runs: [...], bundles: {run_id: bundle}}`.
Все ночи одной серии закрепляются на этом выпуске. Для старого offline-входа
без верхнего pin разрешено вывести его только из одной общей версии всех
bundle; разные версии никогда не складываются в последовательность.
Один bundle содержит `expected_revision`, `initial`, `refills` (все выбранные
дозаполнения), `final`, `outcomes`, `refill_result`, `tail`,
`expected_tail_leagues`, `state`, `capture_window`,
`attempt_history_complete=true` и `window_budget_s=7200`.
`state` — фактический DagRun state: failed даёт FAIL даже при неполных данных,
running/отсутствующий итог дают UNKNOWN. Только success допускает PASS.

`outcomes` содержит **все физические попытки**, их `scope_key`, родительский
`run_id`, `attempt_id`, status, remaining_matches, stop_reason для partial,
фактические started_at/finished_at исполнения. Повтор скоупа в refill имеет
другой attempt_id. Физические Airflow retries имеют прежний логический attempt_id
и отдельные фактические интервалы. Они группируются по плану; проверяется
терминальный исход и отсутствие перекрытий попыток одного плана. Все предыдущие
ошибки остаются в physical_outcomes и failed_physical_attempts, даже после
успешного retry. Отсутствующие попытки и исходы не удаляются из знаменателя.
Записи pending без выбранной работы сохраняются как диагностический контекст,
но не добавляют фиктивную попытку в refills. Финальный snapshot должен отражать
чтение после завершения исполнения. Если его нет, текущей афишей его не заменяют.

Время использования окна — объединение фактических интервалов относительно
7200 секунд активного бюджета,
а не сумма перекрывающихся elapsed_s и не время ожидания TI в пуле. Pending,
open и overdue сохраняются полностью. Проверяются ближайшие core-дедлайны;
debt/disputed не могут обгонять оставшиеся core open-deadline скоупы. Остаток
долга допускает partial только при зафиксированном исчерпании времени окна.
`capture_window.started_at/deadline_at` — сохранённое фактическое начало batch
после schedule sweep и его уже существующий wall deadline с резервом retry.
DagRun started_at остаётся отдельным фактом и определяет ночь. Окно recorder
сохраняется в initial archive; лимиты и retry reserve не изменяются. Нельзя
подменять эти границы началом DagRun или временем нового поручения.
Один зелёный DagRun не заменяет эти проверки.

`tail.evidence` содержит исходные сохранённые scheduler/runner receipts.
`assess_run` повторно вызывает offline `assess_tail`, а не доверяет готовому PASS,
семи `capture_complete=true` или успеху trigger DAG. Проверяются точные семь
турниров из действующего `denominator.tsv`, родитель/child run, версия, физическая
UTC-ночь, успешные season/match TI и пропуск всех per-player capture TI.
Пропуски, failed/partial, чужой run/release/native season и дубликаты сохраняются.
Только три последовательные фактические UTC-ночи из полного expected roster
могут дать 3/3.

Необязательный `third_day` проверяет исходный календарный день **29.09.2026 UTC**.
`original_delivery` содержит первоначальный release
`70446986046371e2adecd034dba66a83afdecf99`, artifact
`fe05605ff876b99a41d7bae740913f41b9432c57737a6cc24ee24912688aec11`
и `delivered_at=2026-09-27T04:20:10Z`. Это фактический DONE исходной доставки.
Поздний recorder release не меняет этот anchor. Поля `day`, реальный `measured_at`
после конца отчётного дня и `checked_at` не раньше измерения сохраняются раздельно.
Например, отчёт за29.09 был измерен30.09 в05:04:49+00:00; точный timestamp29.09
не выдумывается. `lanes.SS` и `lanes.daily` принимают существующие `in24/total`
или `closed/total`. Пустая/отсутствующая полоса даёт UNKNOWN и сохраняет известный
отрицательный результат другой полосы. Реальные SS50/640 и daily0/0 остаются
историческим UNKNOWN. Проверка90% не превращается в три успешных дня.

`third_day_expected_revision` отдельно закрепляет выпуск сохранённого
исторического измерения; `expected_revision` закрепляет текущую серию ночей.
Это не разрешает менять календарный день или исходную доставку. Если в CLI
передан `third_day`, его UNKNOWN/FAIL ограничивает общий итог; `night_status`
сохраняет отдельный результат ночей.

Выход сохраняет отдельные due_scope_completion/window_utilization/tail7,
каждый ожидаемый run, наблюдённые fail/unknown и текущую последовательность N/3.
Exit2 означает отсутствие итогового PASS; файлы сохраняются и при UNKNOWN.

## Точный пакет последующего включения

В рабочей копии добавлены отключённые по умолчанию hooks в
`dag_refresh_sofascore_all_mens.py`. `_plan_refresh_batch` передаёт recorder
тот же полный список, который уже прочитал, до выбора скоупов. Refill recorder
использует только уже выполняемые pending-чтения и сохраняет их до отсечения
blocked скоупов; дополнительных SQL нет. Исходы каждого scope cycle получили
фактические UTC timestamps. При включении сохраняется отдельная неизменяемая
копия каждой физической попытки, прежде чем retry заменит обычный result.
Никаких новых задач, расписаний или source-запросов recorder не создаёт.

После отдельного решения о merge/доставке и включении recorder нужны два
значения в source env, которые экспортирует только изолированный scheduler recipe:

```
SOFASCORE_REFRESH_EVIDENCE_DIR=/opt/airflow/runtime/sofascore/all-men/refresh-evidence
SOFASCORE_EVIDENCE_CODE_REVISION=<фактический полный SHA доставленного merge>
```

Путь использует существующий durable all-men mount. Лимиты, слоты и cron прежние.
До установки обе переменные пусты; recorder ничего не записывает. При включённом
recorder без полного SHA план отклоняется: доказательства не маркируются старой
версией автоматически. После каждой ротации pin нужно квалифицировать заново.
Следует сохранять срок хранения доказательств до приёмки; этот пакет не удаляет
старые файлы и не создаёт уборщик.

После включения нужен естественный завершённый run: полный initial/refill/final
пакет, immutable scope attempts и tail7/export существующих отчётов. До этого
readback producer и unit fixtures не создают t0 или успешные ночи.
Откат — вернуть прежний source release/пустое включение только по разрешённой
процедуре; сохранённые доказательства, failures и серии не удалять.


## Offline сборка семи физических публикаций

`scripts/sofascore_tail_evidence.py --input saved-tail-manifest.json --output tail.json`
читает только перечисленные локальные JSON. Манифест содержит `expected_revision`,
`expected_night` и пути `parent`, `child`, `task_instances`, `import_errors`,
а также `reports` — список путей существующих runner JSON. Относительные пути
разрешаются относительно манифеста. Выход сохраняет SHA256 исходных байтов,
исходные `evidence` и все пробелы. Отсутствующие файлы не скачиваются.

`parent`: `dag_id`, `run_id`, `child_run_id`, `code_revision`, `state`.
`child`: `dag_id`, `run_id`, `code_revision`, `state`, `start_date`, `end_date`,
`logical_date`, исходный `conf` с `competition_ids` и `run_players=false`.
Сохранённый metadb `execution_date` переносится в `logical_date` без округления;
физическая ночь определяется фактическим `start_date`, отдельно от logical date.
Связь parent→child квалифицируется сохранённым trigger/XCom readback.
`task_instances`: полный сохранённый список `dag_id`, `run_id`, `task_id`,
`state`, `try_number`. `import_errors`: `isolate=fotmob`, `code_revision`,
`checked_at` после окончания child и полный `errors` list из изолята.
Самостоятельно выставленные import0/playersfalse не заменяют эти receipts.

Нужны season и match отчёты для каждого из ровно семи турниров.
Season принимает существующий default CLI `entity=all`, а также совместимые
`schedule`/`league_table`; match принимает `match_capture`. Все проверки
публикации и native identity сохраняются для каждого варианта.

Текущий контракт допускает PASS только для первых физических попыток producer TI
(`try_number=1`). Последняя успешная попытка2+ без отдельно квалифицированной
полной истории остаётся UNKNOWN, даже при заявленном `physical_history_complete`.
Этот пакет не вводит новый контракт восстановления retries. Переданные ранние
failed/partial receipts сохраняются в `evidence.reports`; они не заменяются
последним success и не исключаются из исходного пакета.
`evidence_identity` сохраняет `dag_id`, `run_id`, `code_revision`, `logical_date`,
`started_at`, `finished_at`, `league`, `source_tournament_id`, `source_season_id`,
канонический `season` и целый `try_number`, совпадающий с TI.
`publication_receipts` содержит `table`, `rows`, `location` для каждого
**успешного возврата** существующего `save_to_iceberg`. `location` — реальный
`iceberg.bronze.<table>`, а не новый storage snapshot. Verifier сверяет receipts
с исходным `tables` и всеми известными счётчиками строк, включая нулевые:
receipt с12 строками при исходном counter0 отклоняется. Положительные счётчики
по-прежнему требуют receipts; отдельный rejected-row journal не подменяет
публикацию матчей. Для season нужна публикация schedule;
для match — publication capture_status и полный matches_total знаменатель.
`manifest_committed=true` записывается только после существующего flush и
проверки durable manifest. Дополнительных запросов или flush recorder не делает.

Runner сохраняет отдельные immutable физические отчёты в
`$SOFASCORE_REFRESH_EVIDENCE_DIR/daily-tail/<identity-sha256>/<payload-sha256>.json`
до перезаписи обычного result при retry. Включение использует уже описанные
каталог и полный pin. Невалидный pin при включении отклоняется. Идентичность
берётся из существующего args/catalog/env; caller всё ещё квалифицирует pin
по фактической доставке. Старый no-op без физического publication receipt,
out-of-window и ошибка до получения native context не доказывают публикацию
и остаются UNKNOWN. Уже сохранённые неудачные и частичные записи не удаляются.
Пакет не создаёт collector, query, job, t0, новую серию или разрешение доставки.
