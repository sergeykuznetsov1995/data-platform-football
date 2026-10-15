# Локальный учёт закрытых сезонов SofaScore (#1364)

`scripts/sofascore_season_accounting.py` читает сохранённые JSON и создаёт настоящий
JSON-отчёт. Скрипт не запускает inventory collector, HTTP, SQL, Airflow, cron или
сервисы. Установка, доставка и начало приёмки остаются отдельными действиями.
`acceptance_started_at` всегда `null`: отчёт не назначает t0 и не обещает старт
16 октября или следующей ночью.

## Входы и соответствие существующим полям

Один вход содержит `observations` и, при наличии полных доказательств, `days`.
Пути CLI указывают только на локальные копии доказательств и локальный журнал:

```bash
PYTHONDONTWRITEBYTECODE=1 /root/.venvs/dpf-test/bin/python \
  scripts/sofascore_season_accounting.py \
  --input .local/tasks/1364/saved-input.json \
  --journal .local/tasks/1364/season-journal.json \
  --results .local/tasks/1364/results \
  --output .local/tasks/1364/season-days.json
```

Входы не восстанавливаются из `state.completed`, successful scope keys, цвета
worker TI или времени изменения файла. Повторный запуск того же входа не удваивает
события. Код возврата 0 означает успешное построение отчёта, в том числе UNKNOWN
или FAIL; 2 означает ошибку входа или сохранения. Сохраняемый журнал защищён
`flock`, digest и атомарной заменой с fsync файла и каталога. Журнал и предыдущий
отчёт не очищаются при ошибке. При сбое между сохранением журнала и отчёта повтор
восстанавливает отчёт без повторного начисления событий.

| Поле producer | Сохранённое доказательство |
| --- | --- |
| `identity.campaign_id`, `identity.snapshot_id` | Родные campaign/snapshot из того же выпуска snapshot/checkpoint; одинаковы во всём журнале |
| `release` | Фактический код выпуска из доказательства доставки; не дата merge |
| `roster` | Полный перечень принимаемых native `tournament_id`, `season_id`, `group`, включая неизвестные сезоны; происхождение — полный snapshot/denominator и классификация controller до отбора задач |
| `roster_complete` | Явное подтверждение полноты сохранённого roster; selected plan не даёт права поставить `true` |
| `inventory` | Неизменённый результат `scrapers.sofascore.history_inventory.from_rows`/`collect`: `observed_at`, `scopes` |
| `scopes[].tournament_id`, `season_id`, `finished`, `closed`, `schedule_complete`, `ongoing` | Родные поля существующего inventory; строка отсутствует — сезон неизвестен |
| `publication_evidence.definition` | Строка `history_inventory.manifest5+bronze_capture_complete.v1` фиксирует определение существующего `PUBLICATION_CTE`: пять terminal final endpoints и Bronze `capture_complete=true` |
| `publication_evidence.inventory_digest` | SHA256 `json.dumps(inventory, sort_keys=True).encode()`, как в checkpoint `run.inventory_digest` |
| `closed_match_ids` (необязательно) | Родные game IDs законченных, опубликованных матчей **той же** сохранённой выборки; длина списка равна `closed`, дубликаты запрещены |
| `closed_match_runs` (необязательно) | Словарь game ID → точный `EndpointManifest.run_id` завершившей публикации, например `<SOFASCORE_SCOPE_RUN_ID>::targets`, из сохранённого scoped manifest readback и Bronze publication evidence того же native матча |
| `closure_at`, `match_closed_at` (необязательно) | Сохранённые точные времена публикации полного сезона/конкретных матчей; время observation их не заменяет |

Digest подтверждает неизменность сохранённого входа, а не достоверность
произвольного заявления о происхождении. Метка определения разрешена только для
выборки, реально использовавшей существующую проверку manifest+Bronze. Этот
producer не выдаёт успешный scope за такую проверку.

Минимальный baseline, который реально обрабатывается CLI:

```json
{
  "observations": [{
    "observation_id": "baseline-20261015T000000Z",
    "observed_at": "2026-10-15T00:00:00Z",
    "identity": {"campaign_id": "saved-campaign", "snapshot_id": "saved-snapshot"},
    "release": "saved-delivered-sha",
    "roster": [{"tournament_id": 1, "season_id": 2, "group": "2"}],
    "roster_complete": true,
    "inventory": {"observed_at": "2026-10-15T00:00:00Z", "scopes": []},
    "publication_evidence": {}
  }],
  "days": []
}
```

Этот пример даёт baseline с неизвестным сезоном и UNKNOWN серии. Он не доказывает
суточный результат. Для реального baseline нужны полный roster и сохранённые
строки inventory. Нельзя обозначить единственный примерный сезон полным
production roster.

## Переходы сезонов и матчей

Закрытый сезон имеет известный полный schedule, `ongoing=false`, `finished>0`,
`finished=closed` и нулевые `unknown`/`unknown_matches`/`unknown_count`/
`unknown_scopes`, если эти поля присутствуют. Нулевой сезон не увеличивает счётчик.
Уже закрытые сезоны первого baseline никогда не становятся вновь закрытыми.
Позднее producer сохраняет `first_closure`, `reopen`, `reclose`, `evidence_gap`
отдельно. Только первый доказанный переход open→closed увеличивает счётчик сезона.
Unknown→closed не приписывает неизвестный исторический момент текущим суткам.

Каждое событие хранит нижнюю границу `after` и верхнюю `observed_at`. Если обе
границы лежат внутри одних UTC суток, событие можно отнести к этим суткам без
выдуманного точного времени. Если интервал пересёк 00:00 UTC, обе затронутые даты
остаются UNKNOWN без точного сохранённого `closure_at`/`match_closed_at`. Точное
время должно находиться внутри `(after, observed_at]`.

Матч учитывается один раз по `(tournament_id, season_id, game_id)`. Baseline IDs,
повторная публикация после reopen и retry не увеличивают счётчик. Первое появление
после разрыва доказательств сохраняется как baseline, без нового начисления.
Текущий штатный inventory сохраняет **counts**, а не `closed_match_ids` или точные
closure timestamps. Такие сохранённые входы дают полезные переходы сезонов, но
не дают PASS суточного distinct-match темпа. Producer честно возвращает UNKNOWN.
Дополнительные запросы или новый collector этот пакет не запускает.

Для начисленных новых матчей одной связи native season со стартовавшим history
scope недостаточно: параллельный refresh мог опубликовать все матчи. Поэтому
`closed_match_runs[game_id]` должен точно совпасть с
`receipt.items[].environment.SOFASCORE_SCOPE_RUN_ID + "::targets"` **успешного**
history scope того же native сезона. Это соответствует существующему targets
phase run ID (`run_sofascore_scraper.py` проверяет суффиксы `::season`, `::targets`,
`::players`). Временной интервал scope должен пересекать сохранённый интервал
появления матча; точное `match_closed_at`, если есть, должно лежать внутри scope.
Первое закрытие сезона также должно иметь связанную новую history publication.
Публикации refresh, failed history и отсутствующая связь дают UNKNOWN, даже если
native inventory перешёл в полное закрытие. Числа сохраняются как диагностика.
Указанный словарь не выводится из факта старта scope или сходства имени: он
разрешён только из уже сохранённой связи manifest/публикации с конкретным матчем.
В текущем inventory такой связи нет; этот пакет её не собирает задним числом.

## Полные UTC сутки и квитанции

Каждый элемент `days` задаёт:

```json
{
  "day": "2026-10-15",
  "identity": {"campaign_id": "saved-campaign", "snapshot_id": "saved-snapshot"},
  "release": "saved-delivered-sha",
  "group_digest": "sha256-of-full-observation-roster",
  "delivered_interval": {"start": "2026-10-14T12:00:00Z", "end": "2026-10-17T00:00:00Z"},
  "run_roster_complete": true,
  "run_roster_digest": "sha256-of-entire-expected_runs-list",
  "expected_runs": [{
    "run_id": "actual-dag-run-id",
    "release": "saved-delivered-sha",
    "receipt_sha256": "sha256-of-exact-receipt-file-bytes",
    "plan_digest": "existing-receipt-plan-digest",
    "checkpoint_file": "checkpoint-for-that-run.json",
    "checkpoint_sha256": "sha256-of-exact-saved-checkpoint-file-bytes",
    "slot_digest": "existing-checkpoint-run-slot-digest"
  }]
}
```

`expected_runs` должен происходить из полного сохранённого перечня ожидаемых
DagRun, включая failed/missing runs и пересекающие границу суток. Перечень
найденных квитанций не доказывает полноту. Пустой список не квалифицирует день.
Два точных UTC-boundary inventory snapshots должны обрамлять сутки. Все наблюдения
внутри суток обязаны иметь полный roster, известные строки, physical match IDs и
один release. Фактический интервал доставки должен целиком покрывать сутки.
Пересечение двух выпусков даёт UNKNOWN; producer не меняет начало серии.

Для каждого expected run producer вызывает существующий
`dags.utils.sofascore_red_share.history_scope_counts` с сохранёнными
`history-slots-<sha256(run_id)[:20]>.json` и точными scope reports. Проверяются
finalized/digest/индексы, outcomes, native identity и времена. Затем producer
сверяет byte SHA квитанции и сохранённого checkpoint, штатные `plan_digest` и
`slot_digest`, snapshot/campaign, план и выбранную группу. `_read` controller
проверяет существующий schema1 slots checkpoint. Ключи G1/G2 и фактические counts
подтверждают gate99% перед последующими группами. Выбор остальных групп берётся из
того же pinned checkpoint, сделанного существующим controller, а не пересчитывается
по сегодняшнему состоянию. Отсутствие исторического checkpoint даёт UNKNOWN.

Все исходы item сохраняются в `attempts`, включая failed/not_started и количество
запусков. В штатной квитанции `attempts=2` не содержит исход и временные метки
первой внутренней попытки. Без отдельной сохранённой истории retry день UNKNOWN.
Если эта история уже сохранена, `expected_runs[].attempt_histories` задаёт словарь
по строковому `item.index`: список `{status, started_at, finished_at}`. Количество,
хронология, принадлежность интервалу item и последний исход проверяются; неудачная
первая попытка остаётся в отчёте. Producer не реконструирует её из финального
успеха. Backoff внутри удерживаемого слота остаётся в его занятом интервале.

Занятость считается объединением `started_at`–`finished_at` по каждому из трёх
слотов, с обрезкой к одним и тем же UTC суткам. Параллельные интервалы одного
слота не суммируются. Failed/retry интервалы остаются; worker TI не используется.
Знаменатель — `3*86400` slot seconds. Суточный темп — distinct new matches /
`(3*24)` capacity hours, то есть средний темп на слот за полные сутки; простой
не уменьшает знаменатель. Это не измерение отдельного game ID в отдельном слоте.

PASS суток требует совместно ≥67 сезонов, ≥10100 distinct новых матчей,
≥202 матча на capacity hour слота и ≥78% совокупной занятости. Известный полный
день ниже хотя бы одного порога даёт FAIL. Неполное доказательство даёт UNKNOWN;
числа в таком отчёте являются диагностикой известной части, не принятой метрикой.
Серия требует ровно семь разных последовательных UTC дат, одинаковые
campaign/snapshot/group/release и PASS каждого из семи дней. Это расчёт сохранённых
доказательств, не регистрация или изменение живой приёмки.

## Проверки

```bash
env -u PYTEST_ADDOPTS -u PYTEST_PLUGINS \
  PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 \
  /root/.venvs/dpf-test/bin/python -m pytest -q -p no:cacheprovider \
  tests/unit/scrapers/test_sofascore_season_accounting.py \
  tests/unit/scripts/test_sofascore_season_accounting.py
```

Fixture с полным сохранённым набором действительно проходит все четыре порога;
fixture с 10117 матчами при 100% занятости остаётся FAIL по темпу. Проверяются
baseline/partial/zero/unknown, native IDs, повтор финализатора, reopen, UTC-границы,
все три слота, повреждённые/пропущенные квитанции и checkpoint, retry-history,
семь последовательных дней, offline CLI, digest refusal и сбой атомарной замены.
Полный fixture содержит все 67 native scopes, 14606 физических game ID и столько
же точных publication associations; scopes последовательно заполняют три слота.
