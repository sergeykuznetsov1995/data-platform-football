# Локальная квалификация B6 и первого отчёта PR1723

`scripts/fotmob_b6_postflight.py` читает **уже сохранённые** файлы. Он не делает
SQL/HTTP, не запускает Docker, сбор, SLA-reader или доставку. Текущий маршрут —
[изолированный контур без церемонии](fotmob-isolated-ceremony-free.md).
`fotmob_observe.py` и `fotmob_acceptance.py` зависят от церемониальных контрактов
и не заменяют эту проверку. Новый CLI не подключён к автомату доставки и не меняет
его guards или откат.

## Входы и точка checkout

```bash
python3 -B scripts/fotmob_b6_postflight.py \
  --manifest delivery-manifest.json \
  --baseline-probe before-checkout.json \
  --probe after-checkout.json \
  --checkout-evidence checkout-receipt.json \
  --report runner-report.json --report-receipt report-receipt.json \
  --natural-runs natural-runs.json \
  --output first-postflight.json
```

Без трёх аргументов отчёта проверяется только техническая доставка. Наличие
`status=PASS` в таком результате сопровождается `report.status=NOT_CHECKED`.
Если хотя бы один аргумент отчёта указан, нужны все три. Exit 0 означает успешную
квалификацию предоставленных фактов, exit 1 — отказ, exit 2 — отказ записи результата.
Выходной файл создаётся только если его ещё нет. Поздний PASS сохраняют под новым
именем: исходный FAIL и исходные ошибки раннера остаются в истории.

`manifest` — существующий delivery manifest: полный target
`54717e11888550968c15cf893e2d7aa944fa7368`, 30 `runtime_sha256`, два
`scripts_sha256`. `baseline-probe` и `probe` имеют существующий формат captured
`preflight.py`: `checked_at`, `head`, `clean`, `runtime_sha256`, `mounted_match`,
`scripts`, `protected`, `containers`, `cron_sha256`, `ignore_sha256`, `1185`,
`metadb.checked_at/import_errors/dags`. Baseline должен
предшествовать checkout. Protected сравниваются с baseline целиком. Обязательны
валидные captures обоих контейнеров (`id/started/image/mounts`), cron/ignore
SHA256, текст состояния #1185 и булевы `is_paused` всех шести DAG. Эти факты
сверяются с baseline без изменений. Рабочий каталог контейнера capture не
содержит, поэтому CLI его не проверяет. Маркеры автомата (`accepted/inflight/OFF`)
могут штатно меняться. Естественный writer после checkout допустим. Target, 30
хешей и скрипты сверяются с manifest. Шесть DAG должны быть активны и иметь
`last_parsed_time` **строго позже** фактического checkout; import errors — ноль.
Чистый rollback с шестью свежими DAG эту проверку не проходит.

У preflight нет фактического времени checkout. Для него нужен отдельный receipt:

```json
{
  "head": "54717e11888550968c15cf893e2d7aa944fa7368",
  "actual_checkout_at": "2026-10-15T19:50:00Z",
  "checked_at": "2026-10-15T19:51:00Z",
  "evidence_path": "/absolute/retained-runtime-head-reflog.txt",
  "evidence_sha256": "<SHA256 сохранённого файла>",
  "evidence_line": 1
}
```

Это пример формы, а не доказательство состоявшейся доставки. Сборщик доказательств
сохраняет вывод существующего Git HEAD reflog в форме `%H|%gD|%gs` с
`--date=iso-strict`. Выбранная строка должна иметь полный SHA цели и операцию
`checkout: moving from ... to ...`. CLI выводит cut из `HEAD@{ISO timestamp}`,
учитывает указанный offset и сверяет его с receipt. Нужна первая строка свежего
снимка HEAD reflog; более старую запись среди новых CLI отвергает. Время merge, вызова команды,
проверки DAG или сообщения «B6 доставлен» не заменяет checkout. SHA файла
сверяется до разбора строки. CLI доверяет происхождению captured файлов; он не
доказывает подлинность Git журнала и не восстанавливает отсутствующие записи.

## Штатный отчёт и границы результата

`report` — полный исходный JSON раннера, без внешней обёртки `report`. Поля взяты
из существующего runner contract, включая naive `started_at/completed_at`, которые
раннер записывает как UTC. Остальные часы обязаны иметь timezone. Минимальный
failure report без `selection` даёт отказ; его ошибки сохраняются в результате.

`report-receipt` — одна существующая capture-запись: `run` с
`run_id/state/start_date/end_date` из метабазы; `checked_at`, `log_path`,
`log_sha256`, `report_line`, `report_sha256`, `retained_report_line_path` и
`retained_report_line_sha256`. Последние два поля указывают сохранённую исходную
строку Airflow с префиксом `[timestamp] {subprocess.py:106} INFO - <JSON>`.
CLI проверяет SHA этой строки, разбирает JSON, сверяет его целиком с `report` и
проверяет время публикации строки внутри DagRun. Полный runtime log не читается. Runner UUID обязан совпадать с
`fotmob_orchestrated__<UUID>`. Часы отчёта должны лежать внутри DagRun, который
начался после cut. SHA исходного JSON сверяется. Имя файла отчёта не доказывает
DagRun identity. Исходный log path используется только как captured identity,
живой лог CLI не читает.

`natural-runs` — сохранённый список DagRun метабазы в том же формате `run`.
Сборщик должен сохранить полный интервал от cut до выбранного отчёта. CLI требует
первый по `start_date` запуск после cut. Известные `mode=transfers` исключаются;
предшествующий запуск с неизвестным mode консервативно блокирует выбор более
позднего. Два разных запуска с одинаковым первым `start_date` означают
неоднозначность и дают отказ при любом порядке списка. В выбранном отчёте разрешены `refresh`, `daily`, `backfill` и обязательна
сезонная сущность. CLI не запускает forced ingest ради доказательства.

Проверка диагностики сверяет `calendar_unavailable_scopes` только с реально
попытанными `season_bundle` и их `calendar_unavailability`: scope/URL, target hash,
content hash, raw URI, fetched time, HTTP/cache/stale, retryable и следующую
попытку. Обязательства шести календарей сохраняются в полном знаменателе каталога
даже при cooldown и отсутствии в плане текущего окна. Полный
`fotmob-catalog-v1` проверяется существующим `catalog_contract_from_dict`: все
поля, included IDs, scope count/hash и подпись пересчитываются. Подпись и сущности
сверяются с selection, подпись — также с season work plan. Полный проверенный
контракт сохраняется в результате; шесть отдельных обязательств не заменяют
остальной знаменатель каталога. Нельзя подменить их
восстановление зелёным окном, которое их не пыталось собирать. Ноль диагностик
сам по себе допустим и ничего не доказывает о восстановлении источника.

PASS квалификации диагностики допускает `runner.complete=false`. Результат
сохраняет ошибки, попытки, план, завершённые scope и знаменатель. Он **не является**
полной runtime-приёмкой #1699/#1711, SLA PASS, доказательством ремонта источника
или основанием перекрасить исходный first FAIL. Не меняет журнал приёмок.

Тестовая fixture `tests/fixtures/fotmob/b6_postflight/unavailable-report.json`
сокращена из offline report шести настоящих сохранённых source fixtures. UUID
и attempt identities заменены согласованными тестовыми значениями. Полный
контракт исходного offline-каталога сохранён и проходит штатный validator. Это тестовые
данные; они не являются production postflight или новым наблюдением источника.
