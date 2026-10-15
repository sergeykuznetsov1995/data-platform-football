# Хостовая сводка групп истории SofaScore (#1363)

Локальный пакет читает сохранённый `history-controller.json`. Он не запускает
источник, SQL, Airflow или полную утреннюю сводку. Установка на хост — отдельное
разрешение после ревью точного патча. В этом пакете она не выполнялась.

## Контракт

Модуль [history_groups_report.py](../../scrapers/sofascore/history_groups_report.py)
предоставляет `build_report(...) -> dict` и `host_history_groups(...) -> list[str]`.
Второй возвращает ровно одну строку для агрегации хостовой сводки.

Обязательны `campaign_id`, полный `release_id`, `controller_sha256` и путь checkpoint.
`build_report` принимает `release_root`; хостовая функция читает только значение
`SOFASCORE_RELEASE_ROOT` из env-файла. Повтор этой строки, относительный путь,
неверное имя `release-<sha-prefix>`, несовпадение полного detached `.git/HEAD` или
SHA256 контроллера дают UNKNOWN. Символьный HEAD и Git worktree вместо замороженного
release-каталога этим узким адаптером не поддерживаются.

Checkpoint проверяется одним чтением: schema1, кампания, наличие run/plan,
`plan_digest`, а в `mode=slots` — ещё `slot_digest`. Старая batch-схема без `mode`
поддерживается. Обязательны ровно шесть групп: `1`, `2`, `july`, `3`, `4`, `5`.
Проверяются целые неотрицательные счётчики, закрыто ≤ всего, долг, процент,
сумма статусов скоупов и исходный 99% gate. Неизвестные скоупы могут превышать
число известных: это допустимое отражение отсутствующих доказательств.

Строка показывает закрыто/всего, процент, долг, неизвестные, pending+mapping,
parked и quarantine каждой группы. `UNKNOWN` при неизвестных и `BLOCKED` при
непройденном gate печатаются явно. Проверенный ноль имеет статус `EMPTY`.
`0/0` с неизвестными никогда не получает PASS. Общий статус описывает только
эту сводку и не означает завершение приёмки или всех долгов.

`observed_at` берётся из checkpoint, `checked_at` — время чтения. Будущее время,
неуказанный часовой пояс и возраст более `max_age_seconds` дают UNKNOWN для всех
шести групп. Хостовой предел — 86400 секунд; это срок пригодности сводки,
не изменение критериев source-приёмки. При ошибках значения показываются как `?`.
Сырые env-строки, checkpoint и сообщения внешних исключений не печатаются.

Renderer берётся из файла закреплённого релиза после проверки SHA256. Выполняются
только его литеральные `GROUPS`, `LABELS` и `render_summary`; импорты контроллера
и остальной его код не выполняются. Проверка этих констант ограничивает совместимость.
Checkpoint schema1 не содержит release ID. Пины доказывают личность renderer,
но не происхождение наблюдений checkpoint. Его groups находятся вне plan/slot digest;
их семантическая проверка не является криптографической подписью покрытия.

## Offline CLI

```bash
PYTHONDONTWRITEBYTECODE=1 python scripts/sofascore_history_groups_report.py \
  --checkpoint <saved-checkpoint.json> --campaign-id <campaign-id> \
  --release-root <absolute-release-root> --release-id <full-git-sha> \
  --controller-sha256 <sha256> --checked-at <UTC-ISO-timestamp> \
  --max-age-seconds 86400 --json
```

`--json` необязателен. Exit2 означает UNKNOWN; exit0 допускает известный BLOCKED,
поэтому exit0 сам по себе не является успешным gate. CLI ничего не сохраняет.

## Конкретный кандидат установки

Материалы текущего пакета находятся в
`.local/tasks/acceptance-duty/start-readiness-20261014/sofascore-resumed-20261015/execution/groups/`:

- `package-manifest.json` — SHA256 каждого кандидата, исходного и нового host-файла.
- `morning_report.before.py`, `morning_report.after.py`, `host-groups.patch` —
  точный unified patch к сохранённой копии `/root/watchdog/morning_report.py`.
- `host_function.py`, `host_smoke.py`, `host-smoke-result.json` — изолированная
  вставляемая функция, fresh-process проверка и результат.
- `checkpoint.saved.json` — копия уже сохранённого текущего slots checkpoint.
  `release-b1be619f/` — локальная fixture renderer; это не новая установка.
- `red.log`, `focused.log`, `verification.json` — команды, exit и результаты.

Кандидат закрепляет release `b1be619f4b7ba9aa4a908370ae98c0e5339f8551`, controller
SHA256 `40c6bd2a17609984913722c9aee363674b672c0117875eb4caa41a0989d6c920`
и кампанию `9986c4cbe0911ac375a9ba5a2b957c60bee208e810f0016f34bd64f4f95d6d6e`.
Исходный host SHA256 —
`caea58da6d6c4715affbb18d698cfeca1af38f4e7509c81e74cd988c14352481`.

После отдельного разрешения установить точную hash-named копию модуля из manifest
в `/root/watchdog/tools/`, сверить её SHA256 и сохранить проверенную резервную
копию host-файла. До применения патча повторно сверить host SHA256 с manifest:
расхождение требует нового патча и ревью, не применения с fuzzy matching.
Применить только проверенный `host-groups.patch` и сверить итоговый SHA256.
Патч добавляет функцию и единственный вызов сразу после `sofascore_campaign_day()`.
Модуль загружается из уже проверенных байтов, не повторным чтением файла.

Cron, source-каталог, SQL, бюджеты, holds, модели и настройки не меняются.
При смене source release эта фиксированная версия показывает UNKNOWN до
отдельно проверенного обновления пинов. После установки обязательна одна
естественная утренняя сводка с readback; локальный smoke её не заменяет.
Полный unit suite, CI и отдельное ревью остаются условиями последующей PR/доставки.

Откат: проверить, что текущий host-файл совпадает с установленным SHA256, затем
восстановить проверенную pre-install копию по разрешённой процедуре. Hash-named
модуль можно оставить для аудита; после восстановления ссылки на него нет.
Не удалять checkpoint, доказательства, журналы или чужие изменения.
