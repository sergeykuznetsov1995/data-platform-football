# Silver SQL: памятка

Нормативный контракт — [Silver charter](../../../docs/decisions/silver-charter.md),
имена — [canonical naming](../../../docs/decisions/canonical-naming.md),
граница аналитики — [Gold-дизайн](../../../docs/design/gold-star-schema.md).

- Один Bronze-факт остаётся одним Silver-фактом той же детализации. Агрегаты между
  строками и аналитическое объединение источников относятся к Gold.
- Файл содержит SELECT (при необходимости Jinja), без CREATE/INSERT и partitioning:
  таблицу и `_silver_created_at` добавляет `run_silver_transform`.
- Natural key идёт первым; `_bronze_ingested_at` сохраняет lineage. Дедупликация —
  по natural key, последняя `_ingested_at`; при равных значениях сохраняй принятый
  детерминированный tiebreaker, не меняй ключи и hash-PK без проверки потребителей.
- `league`, `season` — последние поля; `season` — varchar slug (`'2425'`).
  Числовой ID при переводе в строку приводи через BIGINT, чтобы не получить `.0`.
- Каждый JOIN к xref ограничен `league` и `season`; иначе размножаются строки.
  Raw и canonical IDs не подменяют друг друга.
- При изменении writer-вызовов сохраняй natural-key дедупликацию и completeness
  guard для `replace_partitions`; неполная выборка не заменяет полную partition.

Проверки и статический `audit_silver_charter.py --check` описаны в
[TESTING.md](../../../TESTING.md). Проход статического аудита не доказывает
правильность данных в Trino. Даты и реестр исключений charter — исторические
решения; новый SQL сверяется с актуальным заданием и потребителями.
