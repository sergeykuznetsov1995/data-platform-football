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

## Класс каждой цели и полнота матчей (#1335)

v15 добавляет `gender_class` (`male`, `female`, `mixed`, `unknown`) и признак
`gender_classified` в существующую сводку. Класс берётся из всех сохранённых
областей provenance вместе с прямым ID; общие player/squad не получают случайный
competition ID. Нет области, реестр не разрешён или ownership ещё неполон —
`unknown`; мужская и женская области вместе — `mixed`. Состояние quarantine
не меняет класс. Запись через ControlStore и обновления registry используют
обновлённую функцию refresh; `pending` и dirty registry по-прежнему запрещают
доверять старой сводке.

DDL не заполняет старые строки и не читает provenance. Лид при согласованной
доставке сначала применяет `migrate` (v13/v14 неизменны), завершает
`backfill-page-scopes`, затем повторяет
`python -m scrapers.fbref.control backfill-page-gender-classes --batch-size 100`
до `done: true`. Новая команда фиксирует один keyset-пакет с checkpoint в одной
транзакции, общим бюджетом 25 с и lock timeout 2 с. При откате checkpoint остаётся
прежним; повтор безопасен, новые цели классифицируются обычным writer. Проверку
нового CHECK добавили как NOT VALID, чтобы DDL не сканировал всю таблицу; новые
и изменённые строки проверяются сразу. Проверки PostgreSQL выполняются отдельно
в разрешённой пустой тестовой БД, здесь они не запускались.

Гейт использует LEFT JOIN от всего frontier: `frontier_gender_counts` и
`frontier_gender_total` включают также quarantined/skipped/dead;
`frontier_gender_classified` считает только готовые сводки без dirty registry.
Для подтверждения 100% покрытия эти два числа должны совпасть; иначе validation
отказывает даже при отсутствии активных утечек. Активные female
и mixed входят в `female_downstream_targets`, unknown — в
`unknown_gender_downstream_targets`. Служебный `competition_index` остаётся
в общем покрытии с классом unknown, но не считается downstream-утечкой: его
назначение — обнаружение турниров, competition scope у него отсутствует.
Orphan player/squad этим исключением не затронуты. Неизвестные области требуют разрешения
provenance/registry, а не назначения male по умолчанию.

Match parser v3 / typed Bronze v5 считает явные два числовых `.score` внутри
scorebox сыгранным матчем, включая 0:0, счёт с отдельными пенальти и числовой
технический результат. Текстовый технический результат без чисел не доказывает
сыгранность. Для такой страницы events и lineups должны содержать строки:
иначе диагностический manifest получает ERROR, прежние typed-данные сохраняются,
успешный `typed:__complete__` не создаётся. Для страницы без счёта прежний контракт
пустых контейнеров сохраняется. Acceptance replay использует тот же parser,
а manifest-гейт проверяет availability и положительное число строк по пометке
`scored_match_requires_rows` на completion.

Неполная страница получает отдельную ошибку `IncompleteMatchPageError`:
dead-letter для неё не применяется. Перечитывание требует нового fetch, очищает
conditional validators, запрещает cross-run импорт прежнего исторического raw
(даже после промежуточной сетевой ошибки) и назначается максимум три раза на цель с задержками
5/10/20 минут. Повтор старого observation не расходует попытку. Уже диагностированный raw
пропускается pre-fetch recovery с сохранением failed-состояния; сбой назначения
refetch остаётся инфраструктурной ошибкой observation и повторяется recovery. Hash, последняя
logical refresh и отсутствие новой lease проверяются под frontier lock; после
исчерпания лимита ошибка остаётся видимой и требует разбора. Обычные бюджеты
fetch и completeness не обходятся.

Старые успешные completion v4 не подтверждают v5: смена typed parser version
включает штатный replay сохранённого raw, без массовых запросов к источнику.
Лишь неполные scored-страницы после replay назначаются на ограниченный fresh
fetch. При доставке проверить backlog и готовность до возобновления приёмки;
полный unit suite, CI и контроль PostgreSQL остаются шагами обвязки/лида.

Legacy-fallback в `dags/utils/silver_tasks.py` оставлен сознательно: существующий
Silver не развивается, новый проектируется отдельно. Ветка действует только
при отсутствии обоих source ID, совпадении canonical league/season,
`eligible_male` и фиксированном `control_run_id`. Ослабления фильтров или миграции
старого Silver в #1335 нет.

Текущий control acceptance SQL использует typed v5; датированный repair-script
`fbref_20260825_repoint_current_season_redirect_aliases.sql` сохраняет v4 как
контракт своих исторических evidence и не предназначен для текущей приёмки.
