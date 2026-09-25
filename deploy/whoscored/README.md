# Контур WhoScored: compose и автомат доставки (#1473)

Изолированный контур WhoScored живёт в `/root/whoscored-1017-runtime` (`$WHOSCORED_RUNTIME_DIR`) и
не зависит от общего стека `data-platform`: свой compose-проект `whoscored-airflow` (своя метабаза
`whoscored-airflow-metadb` + `whoscored-airflow-scheduler`) и `whoscored-gw` (свой
`whoscored_flaresolverr`). С #1473 код в контур ставит `auto_deliver.sh` из этого каталога.

## Файлы

| Файл | Что это |
|---|---|
| `airflow.compose.yaml` | проект `whoscored-airflow`; перенос живого `whoscored-airflow.compose.yaml` 1:1, пути хоста — через `WHOSCORED_RUNTIME_DIR` / `WHOSCORED_PROXY_HOST_FILE` с прежними значениями по умолчанию |
| `gw.compose.yaml` | проект `whoscored-gw` (flaresolverr по digest); перенос `whoscored-gw.compose.yaml` 1:1 |
| `.airflowignore` | белый список DagBag: только `dag_ingest_whoscored.py` и `dag_backfill_whoscored.py` (RE2, дополнением — см. шапку файла) |
| `whoscored.env.example` | имена переменных compose, без значений |
| `auto_deliver.sh` | автомат доставки (ниже) |
| `gateway.compose.yaml`, `systemd/` | платный шлюз #954 и выдача daily — **к контуру не относятся**, этой задачей не тронуты |

## Раскладка контура

```
$RUNTIME/src      git worktree общего .git (/root/data-platform-football), detached на SHA master
                  → /opt/whoscored-src:ro   (PYTHONPATH=/opt/whoscored-src:/opt/whoscored-src/dags)
$RUNTIME/dags     КОПИИ двух DAG + .airflowignore → /opt/airflow/dags:ro
$RUNTIME/{logs,spool,circuit}   состояние контура
```

**Почему дерево в `/opt/whoscored-src`, а не в `/opt/airflow`** (как у FotMob/SofaScore и как написано
в тексте #1473): `dags/scripts/run_whoscored_scraper.py` при `__file__` под `/opt/airflow` требует
вшитого в образ `.pth`-якоря, которого в образе `fbref-590579…` нет, а `runtime_contract.py` при
корне `/opt/airflow` роняет чтение `configs/medallion/competitions.yaml` (`RuntimeContractError`),
потому что `validate_runtime_contract` в master — no-op. Вне `/opt/airflow` раннер ставит мягкий якорь
сам. Смена точки монтирования — только вместе с пересборкой образа, не в этой задаче.

**Почему DAG — копией, а не файловым bind-mount:** `git checkout` пишет новый файл (новый инод), а
файловый маунт остаётся на старом — контейнер молча видит прежний DAG (мина #1263). Каталог `dags/`
смонтирован целиком, скрипт кладёт туда файлы через `cp` во временное имя + `mv`.

## Автомат доставки `auto_deliver.sh`

Ставится **копией** в `/root/whoscored-auto-deliver.sh`. Из `$RUNTIME/src` не запускается (скрипт
отказывается): это worktree, `checkout` подменил бы скрипт посреди работы.

Состояние — `/root/watchdog/state/`: `whoscored-accepted` (SHA в бою, принятый),
`whoscored-accepted-prev`, `whoscored-inflight` (доставка идёт/ждёт приёмки), `whoscored-rejected`
(откаченный SHA master — повторно не ставится, ждём новый коммит), выключатель
`whoscored-auto-deliver.off`, замок `whoscored-deliver.lock`, защёлка суток
`whoscored-auto-deliver-attempted-<UTC-дата>`. Журнал — `/root/whoscored-deliveries/{auto_deliver,journal}.log`,
спасённые патчи — `/root/whoscored-deliveries/rescued-<ts>.patch`. Telegram — `~/.claude/telegram.env`.

### Каждый тик (cron раз в час)

1. Выключатель стоит — выход.
2. **Два законных состояния дерева:** `HEAD` = принятый SHA (или SHA висящей доставки), `git status
   --porcelain` пуст, копии DAG в `$RUNTIME/dags` = файлам дерева. Иначе — патч правок в
   `rescued-<ts>.patch`, Telegram «НУЖНЫ РУКИ», выключатель, выход. Нет базы `whoscored-accepted` —
   тоже выключатель.
3. **Висящая доставка** (`whoscored-inflight`):
   - фаза `deploying` (автомат умер между checkout и перечитыванием) → откат на прежний SHA;
   - фаза `delivered` → ждём первый прогон `dag_ingest_whoscored`, стартовавший после доставки, до его
     конца. Провал = `discover_catalog` или `ingest_daily` не `success`, либо `import_error` > 0, либо
     у DAG `has_import_errors` → **откат** на прежний SHA + 🔴 (SHA помечается `rejected`).
     Задачи `success`, но прирост строк `whoscored_matches` и `whoscored_match_ingest_manifest` = 0 →
     ⚠️ «НУЖНЫ РУКИ» **без отката** (источник мог быть пуст), SHA принимается.
     Иначе ✅ с приростом. `validate_data` в приёмку не входит до #1476 (сейчас красный всегда).
   - Нет прогона 30 ч — напоминание раз в сутки. Trino не ответил — приёмка повторится следующим тиком.
   - Откат ждёт, пока контур занят.
4. **Доставка** — только если висящей нет, в окне **02:00–05:00 UTC**, раз в сутки, в метабазе контура
   нет `dag_run` в `running/queued` (это же покрывает идущий `dag_backfill_whoscored`) и нет живого
   драйвера истории `/root/whoscored_history_backfill/driver.sh`:
   1. `git fetch`, пин = `origin/master`; пин = `rejected` → ждём новый коммит; в путях WhoScored
      (`scrapers/{whoscored,base,utils,__init__.py} dags/utils dags/dag_{ingest,backfill}_whoscored.py
      dags/scripts configs/medallion deploy/whoscored`) нет изменений → «без изменений», выход;
   2. копия автомата = `deploy/whoscored/auto_deliver.sh` пина (иначе стоп: переустановить копию);
      Id образа по тегу из `airflow.compose.yaml` = образу работающего scheduler (тег мог быть
      пересобран); оба DAG пина компилируются; базовые счётчики строк из Trino;
   3. `whoscored-inflight` (фаза `deploying`) — **до** checkout; не записался — бой не трогаем;
   4. `checkout --detach <пин>` → копии двух DAG и `.airflowignore` → import-check в
      `whoscored-airflow-scheduler` (`python -c 'import dag_ingest_whoscored, dag_backfill_whoscored'`
      из `/opt/airflow/dags`) → при смене `dags/utils` — `docker restart whoscored-airflow-scheduler`,
      при смене `airflow.compose.yaml` — `docker compose -p whoscored-airflow … up -d --no-deps
      airflow-scheduler` (смена `gw.compose.yaml` — только уведомление, flaresolverr пересоздаётся
      руками) → ждём перечитывания по метабазе (до 10 мин): оба DAG `has_import_errors = f` и
      `last_parsed_time` > метки + 60 с, `import_error` = 0. airflow CLI не используем — он строит свой
      DagBag с диска;
   5. успех → фаза `delivered`, 🚚; любой провал → откат на прежний SHA тем же порядком, 🔴;
      откат не подтвердился → 🆘, выключатель.

### Ручные режимы

- `--check` — все проверки без единой записи (ни лога, ни замка, ни `git fetch`, ни Telegram;
  `GIT_OPTIONAL_LOCKS=0`): пин — по локальному `origin/master`; окно, занятость, выключатель — только
  заметки. Печатает «к доставке: …» и «проверки пройдены» либо причину стопа (exit 1).
- `--rollback [sha]` — на указанный SHA, без аргумента — на прежний (`whoscored-accepted-prev`, при
  висящей доставке — её `prev`); тем же порядком (checkout → копии → import-check → рестарт при нужде →
  перечитывание). Окно не нужно, но контур должен быть свободен. Успех → `accepted` = цель,
  `accepted-prev` = откуда ушли (второй `--rollback` без аргумента возвращает обратно); если цель не
  `origin/master` — текущий master помечается `rejected`, чтобы ночь не поставила его снова.

## Установка и пересадка контура (руками, после мержа; только свои проекты)

```bash
RT=/root/whoscored-1017-runtime
git -C $RT/src fetch -q origin && git -C $RT/src checkout --detach <sha-мержа>
cp $RT/src/dags/dag_ingest_whoscored.py $RT/src/dags/dag_backfill_whoscored.py $RT/src/deploy/whoscored/.airflowignore $RT/dags/
git -C $RT/src show origin/master:deploy/whoscored/auto_deliver.sh > /root/whoscored-auto-deliver.sh
chmod +x /root/whoscored-auto-deliver.sh
mkdir -p /root/watchdog/state /root/whoscored-deliveries
echo <sha-мержа-полный> > /root/watchdog/state/whoscored-accepted
# пересоздание контура из master-compose: сначала `docker compose config` новых и живых файлов —
# разница только в путях; затем поимённо, общий стек не трогается:
docker compose -p whoscored-airflow -f $RT/src/deploy/whoscored/airflow.compose.yaml \
  --env-file /root/data-platform-football/.env up -d --no-deps airflow-metadb airflow-scheduler
docker compose -p whoscored-gw -f $RT/src/deploy/whoscored/gw.compose.yaml up -d --no-deps whoscored_flaresolverr
/root/whoscored-auto-deliver.sh --check
```

Том метабазы `whoscored_airflow_pgdata` сохраняется. Старые compose-файлы контура — в `$RT/archive/`.

Строка crontab (время хоста не важно — окно проверяется в UTC внутри):

```
15 * * * * /root/whoscored-auto-deliver.sh >> /root/watchdog/whoscored_auto_deliver_cron.log 2>&1
```

Выключить: `touch /root/watchdog/state/whoscored-auto-deliver.off`; включить — удалить файл (после
разбора причины из журнала).
