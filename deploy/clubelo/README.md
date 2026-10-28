# Автомат доставки ClubElo (#1465)

`auto_deliver.sh` раз в ночь довозит из `origin/master` в общее боевое дерево
`/root/dpf-whoscored-merge` **только файлы ClubElo** и проверяет, что планировщик перечитал DAG
без ошибок. При поломке сам возвращает прежние файлы. Копия автомата Understat
(`deploy/understat/`, README там же) с отличиями, перечисленными ниже.

## Что доставляет и чего не делает

- Доставляет: `scrapers/clubelo/**`, `dags/dag_ingest_clubelo.py`, `dags/utils/clubelo_tasks.py`,
  `dags/scripts/run_clubelo_scraper.py` и тесты ClubElo в `tests/` (путь содержит `clubelo`:
  `tests/**/*clubelo*`, `tests/fixtures/clubelo/**`).
- Меняет файл только перезаписью на месте (`cat > файл`): инод и ctime каталога не меняются.
- **Отличие от Understat:** создаёт (A) и удаляет (D) файлы только внутри `scrapers/clubelo/` и в тестах
  ClubElo — так из боя уходит старый `scrapers/clubelo/scraper.py`. Переименование считается как D + A
  (`git diff --no-renames`) и идёт по тем же правилам. Создание/удаление в `dags/`, `dags/utils/`,
  `dags/scripts/` и на верхнем уровне `scrapers/` — стоп «нужны руки».
- НЕ трогает: общие модули (`scrapers/base`, `scrapers/utils`, `dags/utils/config.py`,
  `default_args.py` …), чужие файлы дерева, git дерева (никаких checkout/reset), docker и планировщик
  (только SELECT в метабазу), Trino. Python из боевого дерева не запускает.
- «Код в бою = master» здесь значит: **набор файлов ClubElo** в бою равен их версии в master.

## Окно и занятость

DAG `dag_ingest_clubelo` идёт в :30 каждые 4 ч UTC (`30 */4 * * *`, `execution_timeout` 45 мин),
поэтому окно доставки — 01:30–02:25 UTC, между прогоном 00:30 и 04:30 (вне окна — выход без действий).
Дополнительно: в метабазе нет `task_instance` DAG `dag_ingest_clubelo` в `running`/`queued` и нет
живого процесса `run_clubelo_scraper`. Одна попытка в сутки (защёлка
`clubelo-auto-deliver-attempted-<UTC-дата>`).

## Гейты

Те же, что у Understat (см. `deploy/understat/README.md`, раздел «Гейты и их причины»):
база `clubelo-accepted`, md5 копии автомата против `deploy/clubelo/auto_deliver.sh` в master,
совместимость общих модулей со снимком боя, файлы ClubElo = базе или target и
лишние допускаются только при равенстве target (без `__pycache__`/`.pyc`), компиляция новых `*.py`, приёмка по метабазе
(`dag.has_import_errors = f`, `last_parsed_time > метка + 60 с`, `import_error like '%clubelo%'` = 0,
затем cmp всех файлов ClubElo с master и проверка лишних).

Порядок записи: `scrapers/clubelo/*` (по алфавиту, `__init__.py` последним), затем
`dags/utils/clubelo_tasks.py`, `dags/scripts/run_clubelo_scraper.py`, `dags/dag_ingest_clubelo.py`,
затем тесты, **удаления — в самом конце** (после нового `__init__.py`, который уже не импортирует
удаляемый модуль).

## Совместимость общих модулей

Перед записью автомат распаковывает фиксированный SHA кандидата во временное дерево,
удаляет версии `SHARED_PATHS` из master и заменяет их полным снимком боя.
Дополнительные неиспользуемые shared допустимы; отсутствующая нужная зависимость — отказ.
`CHECK_PY` (по умолчанию `/root/.venvs/dpf-test/bin/python` для cron root) запускает
offline unit-тесты ClubElo (без deploy-тестов),
а также реальные импорты writer и alerts до установки тестовых mocks.
В CI/тестовом стенде `CHECK_PY` передаётся через env как `sys.executable` тестового Python.
Окружение должно содержать зависимости этих тестов. Автозагрузка pytest-плагинов отключена,
пути Python изолированы, сетевые операции запрещены audit hook. Ошибка, отсутствие тестов
или timeout (300 секунд на всю проверку) отменяет доставку. Общие модули не доставляются.
Перед записью повторно проверяются снимок shared, UTC-окно и занятость ClubElo.
DAG unit-тесты используют Airflow stubs; реальное перечитывание проверяет штатная приёмка.

Файлы ClubElo в бою должны совпадать с принятой базой **или** целевым SHA.
Новый относительно базы файл допустим только при совпадении с target. Чужие байты — стоп.
Для смешанного состояния после ручных доставок #1841/#1876 посев не нужен:
`clubelo-accepted` остаётся `1efea301` (полный SHA уже в файле состояния).
Откат сохраняет исходный смешанный набор через `.prev` и проверяет его полный состав и SHA256.

## Приёмка и откат

Как у Understat. Удаляемый файл перед удалением копируется в `.prev-<дата>` (и только после
проверенной копии попадает в список `files` как `D`); откат возвращает его из копии. Созданные
файлы откат удаляет только в `scrapers/clubelo/` и тестах ClubElo.

## Установка (руками, после мержа)

```bash
git -C /root/data-platform-football fetch -q origin
git -C /root/data-platform-football show origin/master:deploy/clubelo/auto_deliver.sh > /root/clubelo-auto-deliver.sh
chmod +x /root/clubelo-auto-deliver.sh
mkdir -p /root/watchdog/state /root/clubelo-deliveries
# посев базы: полный SHA коммита, файлам ClubElo которого СЕЙЧАС равен бой (проверить cmp!).
# Только первичная установка; для ремонта #1880 существующую базу НЕ менять.
# Под замком автомата, после полной сверки файлов:
flock /root/watchdog/state/clubelo-auto-deliver.lock sh -c 'printf "%s\n" "$1" > /root/watchdog/state/clubelo-accepted.tmp && mv /root/watchdog/state/clubelo-accepted.tmp /root/watchdog/state/clubelo-accepted' sh <проверенный-полный-sha>
/root/clubelo-auto-deliver.sh --check
```

Постоянная строка crontab для Europe/Berlin (Debian cron без `CRON_TZ`):

```
40 * * * * /root/clubelo-auto-deliver.sh >> /root/watchdog/clubelo_auto_deliver_cron.log 2>&1
```

Летом 03:40, зимой 02:40 соответствуют 01:40 UTC. Wildcard в поле часов
сохраняет запуск в повторном осеннем часе: Debian cron подавляет повтор только
для заданий с фиксированным временем. Остальные ежечасные запуски отсеиваются
UTC-окном 01:30–02:25; flock и суточная защёлка предотвращают повторную попытку.
Менять cron при переходе времени не нужно.

## Ручные режимы

- `--check` — все проверки без записи: печатает «к доставке: …» и «проверки пройдены» либо причину
  стопа (exit 1). Окно, занятость, выключатель и `inflight` только отмечаются, в Telegram не шлёт,
  защёлку суток не ставит.
- `--rollback <YYYYMMDD>` — возврат доставки из `/root/clubelo-deliveries/<дата>/` (`.prev-<дата>`
  возвращаются, в т. ч. удалённые файлы; созданные — удаляются) + приёмка против исходного снимка (для старых доставок — прежней базы); при успехе
  `clubelo-accepted` ← прежняя база. Подчиняется тому же окну и проверке занятости.

## Что делать по сообщениям Telegram

| Сообщение | Действие |
|---|---|
| ✅ ClubElo: доставлено N файлов | Ничего. Проверить ближайший прогон `dag_ingest_clubelo`. |
| 🔴 доставка отклонена, откачено | Исходные файлы восстановлены (включая смешанное состояние). Разобрать причину в логе, чинить в master; следующая ночь попробует снова. |
| 🆘 откат НЕ подтверждён / незавершённая доставка | Руками: `--rollback <дата>` или ручной возврат, сверить cmp, затем удалить `clubelo-inflight` и `clubelo-auto-deliver.off`. |
| ⛔ ОТМЕНА: совместимость … | Проверить вывод offline-тестов и зависимости test-venv; общий модуль доставлять только по его регламенту. |
| ⛔ нужны руки: сторож каталогов | Новый/удалённый файл вне `scrapers/clubelo/` и тестов: доставить руками по регламенту общего стека, затем пересеять `clubelo-accepted`. |
| ⛔ чужая живая правка | Кто-то правил файл ClubElo в бою. Выяснить, выровнять, пересеять базу. |
| ⛔ автомат не знает базы | Посеять `clubelo-accepted` (см. установку). |
| ⛔ копия автомата отстала от master | Переустановить копию (см. установку). |

## Сторож регистрации и Fixtures (#1466)

Ветка существующего `dag_ingest_clubelo`: понедельник, слот **00:30 UTC / 03:30 МСК**,
либо ручной `{"run_watch": true}`. Проверяется `data_interval_end`, потому что logical_date
cron-прогона указывает на предыдущий слот. История (`run_history=true`) и обычные внешние
триггеры сторожа не запускают. Запросы начинаются после успешного `scrape_daily`;
после ошибки или блокировки дневного сбора дополнительных запросов сторожа нет;
`validate_data` остаётся независимой конечной задачей, сохраняющей ошибки дневного сбора.

Существующий runner поддерживает `--mode watch`, `--state-file` и `--source html`.
Используются транспорт ClubElo и его `get_source`; `api` остаётся заглушкой и не делает
запросов. Сторож запрашивает только `/login/` и `/Fixtures`, учитывает запросы и `wire_bytes`;
после 403/429/Cloudflare-блокировки дальнейших запросов нет. Архив и таблицы сбора не меняет.

Состояние и lock: `/opt/airflow/logs/clubelo_registration_watch.json{,.lock}` на существующем
постоянном томе logs. `observed` хранит последние достоверные наблюдения, `notified` —
состояние после принятого Telegram-сообщения; начальная закрытая регистрация и пустая Fixtures
становятся тихой базой. Изменение регистрации/видимых цен или наличия строк матчей вызывает
одно сообщение. CSRF, время создания страницы, число матчей и боковые рейтинги клубов не
входят в сравнение. Недоступность страницы не означает открытие регистрации или исчезновение
матчей. При неудачной отправке сообщение остаётся ожидающим до следующей проверки.

Обычные ошибки сохраняются в результате и логах; завершающая `finish_watch` также сообщает
о сбое/таймауте процесса без падения успешного daily. Повреждённый state не перезаписывается;
ошибка сохранения до отправки запрещает сообщение. Как у обычной файловой дедупликации,
аварийное завершение между принятием Telegram и сохранением `notified` может вызвать повтор
при следующей проверке: Telegram не предоставляет транзакцию с локальным файлом.

Offline-проверки: `tests/unit/scrapers/test_clubelo_watch.py`, дополнения к тестам DAG и runner,
синтетические `tests/fixtures/clubelo/watch/`. Настоящий Airflow и отдельная временная SQLite:
`tests/integration/clubelo_watch_airflow_check.py` запускается **как скрипт** только в
изолированном контейнере с read-only `/workspace`, сетью `none` и `AIRFLOW_HOME`/БД в `/tmp`.
Он проверяет импорт, пропуск ветки и итоговые статусы DagRun; production не использует.

Для сдачи #1466 ещё нужны два боевых запуска, проверка файла состояния и одно уведомление
при изменении. Разработка/PR не разрешают merge, доставку или обход блокера #1465.

## Файлы автомата

- Лог: `/root/clubelo-deliveries/auto_deliver.log`; журнал исходов: `/root/clubelo-deliveries/journal.log`;
  копии доставки: `/root/clubelo-deliveries/<дата>/` (`*.prev-<дата>`, `*.absent-<дата>`, `*.new`,
  `files`, `accepted-before`).
- Состояние: `/root/watchdog/state/clubelo-accepted`, `clubelo-inflight`, `clubelo-auto-deliver.off`,
  `clubelo-auto-deliver-attempted-<дата>`, `clubelo-inflight-reminded-<дата>`, замок `clubelo-auto-deliver.lock`.
- Тест: `tests/unit/deploy/test_clubelo_delivery_script.py`.

## Отдельный шлюз Cloudflare (#1841)

`gateway.compose.yaml` — отдельный проект `clubelo-gw`, внешняя `dp-backend`.
Фильтр запускается из целиком пинованной read-only копии репозитория вне
`/opt/airflow`; образ должен быть закреплён digest. Ledger и логи принадлежат
только этому шлюзу. Общий фильтр, scheduler и его runtime-пломбы не меняются.
`PROXY_POOL_JSON` обязателен: fallback на `proxys.txt` выключен.

Образ шлюза должен быть **без вшитого WhoScored runtime-якоря**: образ общего
фильтра `ws954` на изменённом `filter_proxy.py` завершается с exit 78, даже при
переопределённом entrypoint. По пробе лида подходит образ SofaScore-шлюза:
`data-platform-airflow-scheduler:fbref-590579ef…`, image ID `sha256:5286d9cf18bf…`.
Это сокращённые идентификаторы из handoff; перед доставкой лид должен установить
полный digest проверенного образа в `CLUBELO_GATEWAY_IMAGE`.
Браузеру разрешён только дополнительный `challenges.cloudflare.com:443`;
`turnstile.cloudflare.com`, `android.clients.google.com` и `www.google.com` закрыты.

Параметры для лида перечислены в `env.example`: отдельные токены
`PROXY_FILTER_CONTROL_TOKEN` и `CLUBELO_PROXY_CONTROL_TOKEN` (разные, ≥32 символов),
пул, образ, release/state paths, daily/history caps, дневной потолок, lease и URL
caps, TTL. Daily cap 32 МиБ покрывает bootstrap и новые клубы; history cap и
lease/URL cap 128 МиБ покрывают примерно 85 МБ истории плюс bootstrap. Это общий
учёт всех туннельных байтов браузера и requests, а не размер DOM. Дневной потолок
4096 МиБ — конфигурация отдельного шлюза; алгоритм учёта не меняется.
TTL 3600 секунд покрывает таймауты задач (daily 45 мин, history 30 мин).

Порядок доставки лидом после проверки условий хоста:

1. Подготовить пинованную копию с `scripts/fbref_proxy/filter_proxy.py` и соответствующим
   `scrapers/whoscored/runtime_contract.lock`, проверить образ, сеть, права и отдельный
   каталог state, заполнить env вне git. Живые пломбы общего стека не менять.
2. Запустить только шлюз из его release root:
   `docker compose -p clubelo-gw -f deploy/clubelo/gateway.compose.yaml --env-file <gateway.env> up -d --no-deps clubelo_proxy_filter`.
3. Проверить цепочку lease → FlareSolverr `/Ranking` → requests `/Results` через тот
   же lease. Проверить строгий allowlist `clubelo.com:443` и `challenges.cloudflare.com:443`, стабильность выхода,
   фактический расход и освобождение lease. Успешная проба напрямую через
   резидентный прокси пока не доказывает работу через allowlist шлюза.
4. Записать токен ClubElo в уже смонтированный ro-каталог: файл
   `configs/proxy_filter/clubelo_control_token` (секрет вне git, читаемый пользователем
   задачи). Коллектор использует env-токен, иначе `CLUBELO_PROXY_CONTROL_TOKEN_FILE`,
   по умолчанию `/opt/airflow/configs/proxy_filter/clubelo_control_token`.
   URL по умолчанию `http://clubelo_proxy_filter:8899`, solver — `http://flaresolverr:8191`.
   Пересоздание общего scheduler и изменение его env не нужны. Для lease обязательны
   настоящие `AIRFLOW_CTX_DAG_ID`, `AIRFLOW_CTX_DAG_RUN_ID`, `AIRFLOW_CTX_TASK_ID`; CLI без них при challenge остановится.
5. Доставить только ClubElo штатным автоматом, дождаться естественного DAG и
   снимка `clubelo_rank_snapshot` за дату рейтинга сайта. До этого приёмка #1841
   не завершена. Общий фильтр перезапускать не требуется.

Первый запрос прямой, без proxy из окружения. Только `403` вместе с
`cf-mitigated: challenge` разрешает один bootstrap. Браузер передаёт cookies/UA,
а raw по-прежнему сохраняется из HTTP-ответа requests. Повторная блокировка,
невалидный clearance или ошибка lease/solver останавливают сбор.
