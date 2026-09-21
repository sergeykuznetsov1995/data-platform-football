# FBref: возврат цели из dead-letter

Что это. С #1317 одна запись, упавшая после взятия lease, больше не валит всю
волну: цель уходит в карантин (`page_frontier.state = 'quarantined'`), а волна
продолжается. Такой карантин **обратим** — в отличие от контрактного
(`last_error_message` без префикса), который выносится только по доказанной
идентичности архивной страницы и снимается лишь когда источник изменил форму.

Признак dead-letter: `last_error_class = 'ParseContractQuarantined'` **и**
`last_error_message LIKE 'dead_letter:%'`. Формат причины:

```
dead_letter:<ТипИсключения>:review_after=<YYYY-MM-DD>:<первые 200 символов текста>
```

Дата `review_after` — старт рабочей волны + 7 дней. Отдельной колонки под неё в
`page_frontier` нет (схема control-БД в это окно не менялась); поле появится
вместе с ближайшей миграцией. Счётчик попыток тоже производный — см. ниже.

## Что видно без БД

- `dead_lettered` в итоговой строке рана (`WaveResult.as_dict()`), в дренаже
  `run_recovery_wave` и в сообщении mass-guard.
- Строка лога `Dead-lettered <target_id> after <ТипИсключения>: <причина>`.
- Карантинная цель остаётся `stale` для гейта свежести (урок 72/83), поэтому
  ран может остаться красным даже после того, как волна выжила.

## Посмотреть список (только SELECT)

```sql
SELECT target_id,
       page_kind,
       refresh_policy,
       last_fetched_at,
       last_content_hash,
       last_error_message,
       substring(last_error_message from 'review_after=(\d{4}-\d{2}-\d{2})')
         AS review_after
FROM fbref_control.page_frontier
WHERE state = 'quarantined'
  AND last_error_message LIKE 'dead_letter:%'
ORDER BY review_after NULLS LAST, target_id;
```

Счётчик попыток по цели (колонки нет, считается по журналу обработки):

```sql
SELECT count(*) AS failed_attempts
FROM fbref_control.observation_processing
WHERE target_id = :target_id
  AND status = 'failed';
```

## Вернуть цель в очередь

Запускает **только владелец** — это единственная запись в control-БД в этом
сценарии. Одна цель за раз; сначала посмотреть причину из списка выше и решить,
изменилось ли что-то (новый код разбора, новые байты у источника).

```sql
UPDATE fbref_control.page_frontier
SET state = 'queued',
    next_fetch_at = now(),
    retry_after = NULL,
    last_error_class = NULL,
    last_error_message = NULL,
    updated_at = clock_timestamp()
WHERE target_id = :target_id
  AND state = 'quarantined'
  AND last_error_message LIKE 'dead_letter:%'
RETURNING target_id, state, next_fetch_at;
```

Фильтр по префиксу обязателен: без него тот же UPDATE поднимет и контрактный
карантин, и scope-карантин, которые снимаются по другим правилам.

Проверка, что вернулось ровно то, что хотели: `RETURNING` должен отдать одну
строку. Ноль строк — цель уже не в dead-letter (кто-то вернул раньше или это
карантин другого вида).

## Если цель возвращается в dead-letter снова

Повторный dead-letter на тех же байтах (`last_content_hash` не изменился) — это
не повод возвращать её третий раз, а повод чинить разбор: причина уже названа в
`last_error_message`. Возврат имеет смысл, когда изменился код (новая версия
парсера) или когда у цели появились свежие байты.
