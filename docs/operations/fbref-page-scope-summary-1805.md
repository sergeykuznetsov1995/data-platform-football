# FBref: сводка областей страниц (#1805)

`PAGE_AGES_SQL` читает `page_scope_summary`, frontier, manifest и очередь инвалидации;
`frontier_provenance` в запросе отчёта отсутствует. Семантика ownership сохранена
неизменяемым SQL snapshot v13; сборщик использует тот же rollup. Parser version,
идентичность batch и append-only provenance не меняются.

## Переход и проверка (выполняет лид в согласованном окне)

Этот документ — процедура, а не доказательство доставки или разрешение на неё.
В worktree выполнены только offline-проверки; PostgreSQL/Trino и production-замеры
не выполнялись. Следовать действующему host handoff и регламенту источника.

1. Проверить PostgreSQL-фикстуру на **отдельной пустой тестовой БД** с
   `FBREF_SCOPE_TEST_POSTGRES_URI`: файл
   `tests/integration/scrapers/test_fbref_page_scope_summary.py`. Fixture отказывается
   работать с существующей схемой `fbref_control`, создаёт свою и удаляет её после
   проверки. Использовать штатный pytest-профиль с выключенным plugin autoload.
2. Миграцию v13 и код worker/report доставлять согласованно: старый
   `validate_migrations()` отвергает новую версию схемы. Штатная команда
   `python -m scrapers.fbref.control migrate` создаёт таблицы, функции и триггеры;
   она **не читает старый provenance и не заполняет старые target**.
3. После включения поддержания сводки повторять
   `python -m scrapers.fbref.control backfill-page-scopes --batch-size 100`
   до JSON `done: true`. Одна команда — одна транзакция; checkpoint фиксируется
   вместе с её результатом. Размер 1–1000; общий бюджет пакета — 25 секунд,
   ожидание lock — до 2 секунд, `work_mem=4MB`. При таймауте пакет полностью
   откатывается; уменьшить размер и повторить. Источник читается отдельно для
   каждого `child_target_id`, с проверкой `EXPLAIN` **без ANALYZE**; Seq Scan
   отвергается. Полные проходы по provenance запрещены.
4. До заполнения всех подходящих под cutoff target отчёт выдаёт отдельную ошибку
   `scope_summary_incomplete`, а не считает отсутствующие строки orphan/нулём.
   Новые target уже получают полную сводку; запись старого target до backfill
   не выдаёт частичную историю за полную. Повтор завершённой команды — no-op.
5. Приложить к issue #1805 read-only замеры обоих запросов с обычным лимитом 30 с.
   Для page ages предварительно проверить `EXPLAIN` без ANALYZE: provenance в плане
   отсутствует. Не увеличивать таймаут для тяжёлых экспериментов. Полный unit suite,
   обязательные CI и эти замеры остаются условиями сдачи.

## Запись и реестры

Исходные области provenance дедуплицируются до aliases; `source_ids` берутся из
текущей frontier-строки отдельно. SQL NULL и пустые строки не нормализуются.
Триггеры сохраняют области/инвалидацию в транзакции исходной записи; ControlStore
пересчитывает флаги один раз на target в discovery batch. Они охватывают обычную
запись, discovery, replay и normalization. Изменения state/URL/fetch читаются прямо
из frontier и не требуют пересчёта областей.

Порядок блокировок: writer сначала захватывает полный набор существующих frontier
(включая владельцев входящих URL), затем shared advisory gate, затем summary.
Registry helper сначала заканчивает свои изменения реестра/frontier, затем берёт
exclusive gate и обновляет затронутые summary из сохранённых областей, пакетами по
250. Это не сканирует provenance. Bound remediation откладывает этот шаг до конца
общей транзакции. Косметические изменения реестра (например, `last_seen_at`) не
инвалидируют флаги; gender/current/lifecycle/present/crawl_state, используемая
metadata и перепривязка alias — инвалидируют.

Прямые SQL-изменения реестра тоже оставляют durable invalidation. После них нужна
команда `python -m scrapers.fbref.control refresh-page-scopes`; пока очередь не
обработана, отчёт отказывает явно. `ON CONFLICT DO UPDATE` сериализует повторную
инвалидацию с её обработкой. Прямые изменения frontier/provenance вне ControlStore
оставляют `pending`; их завершает keyset-backfill (для исправления уже пройденного
target нужен согласованный отдельный repair, не обход completeness).

SQL сводки и snapshot находятся в существующем `control/migrations.py`.
Runtime-манифесты FotMob и WhoScored сохранены без изменений; новых модулей
в их контракт доставки не добавлено. Runtime seals/lock-файлы не переписаны.
Схему v13/её SQL snapshot после выпуска не редактировать — изменения делать новой
миграцией.

## Сезонные таблицы

Наличие `results*_overall` на странице season подтверждается inventory независимо
от `availability`; текущий hash, успешный page manifest и cutoff сохраняются.
Парсер здесь не переклассифицируется. `host_trino` добавляет служебную строку к
SELECT и строго проверяет маркированный CSV, поэтому успешный пустой результат
становится `[]`; `check=True`, настоящие ошибки и повреждённый ответ не скрываются.
Host wrapper вне репозитория не меняется.
