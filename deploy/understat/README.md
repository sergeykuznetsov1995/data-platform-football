# Автомат доставки Understat (#1432)

`auto_deliver.sh` раз в ночь довозит из `origin/master` в общее боевое дерево
`/root/dpf-whoscored-merge` **только файлы Understat** и проверяет, что планировщик перечитал DAG
без ошибок. При поломке сам возвращает прежние файлы.

## Что доставляет и чего не делает

- Доставляет: `scrapers/understat/**`, `dags/dag_ingest_understat.py`, `dags/dag_backfill_understat.py`,
  `dags/utils/understat_tasks.py`, `dags/scripts/run_understat_scraper.py`.
- Меняет файл только перезаписью на месте (`cat > файл`): инод и ctime каталога не меняются.
  Новые файлы создаёт только внутри `scrapers/understat/`.
- НЕ трогает: общие модули (`scrapers/base`, `scrapers/utils`, `dags/utils/config.py`,
  `default_args.py` …), чужие файлы дерева (грязные файлы FBref), git дерева (никаких checkout/reset),
  docker и планировщик (только SELECT в метабазу), Trino. Python из боевого дерева не запускает.
- «Код в бою = master» здесь значит: **набор файлов Understat** в бою равен их версии в master.

## Окно и занятость

Окно 11:00–08:30 UTC (вне него — выход без действий). Дополнительно: в метабазе нет
`task_instance` DAG `dag_ingest_understat`/`dag_backfill_understat` в `running`/`queued` и нет живого
процесса `run_understat_scraper`. Занятость меряется по задачам, а не по `dag_run.state`: прогон
истории на паузе висит в `running` на сенсоре (`up_for_reschedule`) и доставке не мешает.
Одна попытка в сутки (защёлка `understat-auto-deliver-attempted-<UTC-дата>`).

## Гейты и их причины

| Гейт | Почему |
|---|---|
| Файл `understat-accepted` = полный SHA master, которому равен бой | Без базы автомат не знает, что в бою, и не может отличить чужую правку. |
| Копия автомата = `deploy/understat/auto_deliver.sh` в master (md5) | Иначе ночью работает старая логика. |
| Общие модули в бою = master (cmp по каждому файлу) | Файлы Understat импортируют `scrapers.base.*`, `utils.config`, `utils.default_args`; при расхождении новый код Understat может не сойтись с общими модулями боя. Стоп, exit 1. |
| Все файлы Understat в бою = принятой базе | Иначе в бою чужая живая правка — перезаписать её молча нельзя. |
| В diff только `M` и `A` внутри `scrapers/understat/` | Сторож WhoScored (`scrapers/whoscored/runtime_contract.py`) роняет разбор DAG всей платформы, если ctime каталогов `dags/`, `dags/utils/`, `scrapers/`, `scrapers/base/`, `scrapers/utils/` новее старта процесса. Создание/удаление/переименование там меняет ctime. `D`, `R`, `C`, `A` вне `scrapers/understat/` (в т.ч. новые `dags/**/*understat*.py`) — стоп, «нужны руки». |
| Новые байты `*.py` компилируются (`python3 -I -S`, строкой, без .pyc) | Синтаксическая ошибка не доезжает до боя. |
| Приёмка по метабазе | airflow CLI строит свой DagBag с диска и лжёт. `import_error` общей метабазы никогда не 0 (строки WhoScored), поэтому смотрим только свои DAG. |

Порядок записи: `scrapers/understat/*` (по алфавиту, `__init__.py` последним), затем
`dags/utils/understat_tasks.py`, `dags/scripts/run_understat_scraper.py`, `dags/dag_backfill_understat.py`,
`dags/dag_ingest_understat.py` — импортируемые раньше импортирующих.

## Приёмка и откат

После записи (до 6 мин, опрос раз в 20 с): для обоих DAG `dag.has_import_errors = f` и
`dag.last_parsed_time > момент записи`; `import_error where filename like '%understat%'` = 0; затем
cmp всех файлов Understat в бою с master пустой. Успех → `understat-accepted` ← SHA.

Провал → возврат `.prev`-копий (`cat >`), удаление созданных файлов (только в `scrapers/understat/`),
повтор приёмки против прежней базы. Подтверждён → 🔴 в Telegram. Не подтверждён → 🆘, ставится
выключатель `understat-auto-deliver.off`, маркер `understat-inflight` остаётся.

## Установка (руками, после мержа)

```bash
git -C /root/data-platform-football fetch -q origin
git -C /root/data-platform-football show origin/master:deploy/understat/auto_deliver.sh > /root/understat-auto-deliver.sh
chmod +x /root/understat-auto-deliver.sh
mkdir -p /root/watchdog/state /root/understat-deliveries
# посев базы: полный SHA master, файлам Understat которого СЕЙЧАС равен бой (проверить cmp!)
echo <полный-sha> > /root/watchdog/state/understat-accepted
/root/understat-auto-deliver.sh --check
```

Строка crontab (время хоста — CEST, UTC+2):

```
10 3 * * * /root/understat-auto-deliver.sh >> /root/watchdog/understat_auto_deliver_cron.log 2>&1
```

= 01:10 UTC; после перехода на зимнее время 25.10 — 02:10 UTC, всё ещё в окне.

## Ручные режимы

- `--check` — все проверки без записи: печатает «к доставке: …» и «проверки пройдены» либо причину
  стопа (exit 1). Окно, занятость, выключатель и `inflight` только отмечаются, в Telegram не шлёт,
  защёлку суток не ставит.
- `--rollback <YYYYMMDD>` — возврат доставки из `/root/understat-deliveries/<дата>/` (`.prev-<дата>`,
  созданные файлы удаляются) + приёмка против прежней базы; при успехе `understat-accepted` ← прежняя база.

## Что делать по сообщениям Telegram

| Сообщение | Действие |
|---|---|
| ✅ доставлено N файлов | Ничего. Утром проверить прогон `dag_ingest_understat` 09:00. |
| 🔴 доставка отклонена, откачено | Бой на прежней базе. Разобрать причину в логе, чинить в master; следующая ночь попробует снова. |
| 🆘 откат НЕ подтверждён / незавершённая доставка | Руками: `--rollback <дата>` или ручной возврат, сверить cmp, затем удалить `understat-inflight` и `understat-auto-deliver.off`. |
| ⛔ ОТМЕНА: общий модуль … ≠ master | Общий модуль в бою разошёлся с master — доставка общего модуля идёт не через этот автомат. Ждать выравнивания или решать руками. |
| ⛔ нужны руки: сторож каталогов | Новый/удалённый/переименованный файл: доставить руками по регламенту общего стека (с рестартом), затем пересеять `understat-accepted`. |
| ⛔ чужая живая правка | Кто-то правил файл Understat в бою. Выяснить, выровнять, пересеять базу. |
| ⛔ автомат не знает базы | Посеять `understat-accepted` (см. установку). |
| ⛔ копия автомата отстала от master | Переустановить копию (см. установку). |

## Где что лежит

- Лог: `/root/understat-deliveries/auto_deliver.log`; журнал исходов (строка на запуск с записью):
  `/root/understat-deliveries/journal.log`; копии доставки: `/root/understat-deliveries/<дата>/`
  (`*.prev-<дата>`, `*.absent-<дата>`, `*.new`, `files`, `accepted-before`).
- Состояние: `/root/watchdog/state/understat-accepted`, `understat-inflight`, `understat-auto-deliver.off`,
  `understat-auto-deliver-attempted-<дата>`, `understat-inflight-reminded-<дата>`, замок `understat-auto-deliver.lock`.
- Тест: `tests/unit/deploy/test_understat_delivery_script.py`.
