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

`tail` имеет родной run_id, night (UTC), тот же code_revision, run_players=false,
fotmob_import_errors=0 и ровно семь строк `leagues` для **полного**
`expected_tail_leagues`: league, status, capture_complete. Эти статусы требуют
исходных отчётов/публикации, не только состояния trigger DAG. Пропуск лиги,
неизвестный импорт или провал сохраняются. Только три последовательные
фактические UTC-ночи из полного expected roster могут дать 3/3.

Необязательный `third_day` содержит реальную delivered_at, measured_at на
границе delivery+72h, checked_at не раньше факта, code_revision и отдельные
`lanes.SS`/`lanes.daily` с closed/total. Нулевая полоса или неизвестная граница
даёт UNKNOWN. Проверка 90% не превращается в три успешных дня; дата нового
поручения или merge не заменяет первоначальную доставку.

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
