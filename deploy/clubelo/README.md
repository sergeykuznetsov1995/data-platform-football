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
общие модули в бою = master (по байтам и по составу), все файлы ClubElo в бою = принятой базе и
лишних нет (без `__pycache__`/`.pyc`), компиляция новых `*.py`, приёмка по метабазе
(`dag.has_import_errors = f`, `last_parsed_time > метка + 60 с`, `import_error like '%clubelo%'` = 0,
затем cmp всех файлов ClubElo с master и проверка лишних).

Порядок записи: `scrapers/clubelo/*` (по алфавиту, `__init__.py` последним), затем
`dags/utils/clubelo_tasks.py`, `dags/scripts/run_clubelo_scraper.py`, `dags/dag_ingest_clubelo.py`,
затем тесты, **удаления — в самом конце** (после нового `__init__.py`, который уже не импортирует
удаляемый модуль).

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
# На 29.09.2026 это eefa5e0239bb3ab4e80e0217756f286803118013 (17.07).
echo <полный-sha> > /root/watchdog/state/clubelo-accepted
/root/clubelo-auto-deliver.sh --check
```

Строка crontab — 01:40 UTC. Cron хоста идёт по времени Europe/Berlin (Debian cron, `CRON_TZ` не
поддерживается), поэтому строка зависит от сезона:

```
# летнее время (CEST, UTC+2) — до 25.10.2026:
40 3 * * * /root/clubelo-auto-deliver.sh >> /root/watchdog/clubelo_auto_deliver_cron.log 2>&1
# зимнее время (CET, UTC+1) — с 25.10.2026: 40 2 * * *
```

Без смены строки 25.10 запуск уедет на 02:40 UTC — вне окна, автомат будет молча выходить.

## Ручные режимы

- `--check` — все проверки без записи: печатает «к доставке: …» и «проверки пройдены» либо причину
  стопа (exit 1). Окно, занятость, выключатель и `inflight` только отмечаются, в Telegram не шлёт,
  защёлку суток не ставит.
- `--rollback <YYYYMMDD>` — возврат доставки из `/root/clubelo-deliveries/<дата>/` (`.prev-<дата>`
  возвращаются, в т. ч. удалённые файлы; созданные — удаляются) + приёмка против прежней базы; при успехе
  `clubelo-accepted` ← прежняя база. Подчиняется тому же окну и проверке занятости.

## Что делать по сообщениям Telegram

| Сообщение | Действие |
|---|---|
| ✅ ClubElo: доставлено N файлов | Ничего. Проверить ближайший прогон `dag_ingest_clubelo`. |
| 🔴 доставка отклонена, откачено | Бой на прежней базе. Разобрать причину в логе, чинить в master; следующая ночь попробует снова. |
| 🆘 откат НЕ подтверждён / незавершённая доставка | Руками: `--rollback <дата>` или ручной возврат, сверить cmp, затем удалить `clubelo-inflight` и `clubelo-auto-deliver.off`. |
| ⛔ ОТМЕНА: общий модуль … ≠ master | Общий модуль в бою разошёлся с master. Ждать выравнивания или решать руками. |
| ⛔ нужны руки: сторож каталогов | Новый/удалённый файл вне `scrapers/clubelo/` и тестов: доставить руками по регламенту общего стека, затем пересеять `clubelo-accepted`. |
| ⛔ чужая живая правка | Кто-то правил файл ClubElo в бою. Выяснить, выровнять, пересеять базу. |
| ⛔ автомат не знает базы | Посеять `clubelo-accepted` (см. установку). |
| ⛔ копия автомата отстала от master | Переустановить копию (см. установку). |

## Где что лежит

- Лог: `/root/clubelo-deliveries/auto_deliver.log`; журнал исходов: `/root/clubelo-deliveries/journal.log`;
  копии доставки: `/root/clubelo-deliveries/<дата>/` (`*.prev-<дата>`, `*.absent-<дата>`, `*.new`,
  `files`, `accepted-before`).
- Состояние: `/root/watchdog/state/clubelo-accepted`, `clubelo-inflight`, `clubelo-auto-deliver.off`,
  `clubelo-auto-deliver-attempted-<дата>`, `clubelo-inflight-reminded-<дата>`, замок `clubelo-auto-deliver.lock`.
- Тест: `tests/unit/deploy/test_clubelo_delivery_script.py`.
