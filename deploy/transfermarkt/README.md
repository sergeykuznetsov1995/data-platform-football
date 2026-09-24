# Контур Transfermarkt (#1387)

Свой Airflow (`transfermarkt-airflow`: metadb, init, scheduler, UI по профилю `ui` на
127.0.0.1:8084) и свой платный шлюз (`transfermarkt-gw`: `transfermarkt_gw`,
`filter_proxy.py --source-mode transfermarkt-only`, без суточного бюджета). Код — из
замороженного дерева `/opt/transfermarkt/releases/release-<sha8>`; логи и состояние —
в `/root/transfermarkt-runtime`, вне дерева. Активны 4 DAG (`.airflowignore`):
ingest и discover работают, backfill и silver на паузе.

## Установка (один раз, ничего на общем стеке)

1. `docker network create transfermarkt-net`
2. `install -d -m 0755 /etc/data-platform /opt/transfermarkt/releases`;
   `install -d -m 0700 /root/transfermarkt-runtime/auto-deliver`;
   `install -d -o 50000 -g 0 -m 0750 /root/transfermarkt-runtime/logs /root/transfermarkt-runtime/gateway-state`
3. `/etc/data-platform/transfermarkt.env` (root, 0600) по `transfermarkt.env.example`:
   образы = образы контура SofaScore, `TRANSFERMARKT_AIRFLOW_DB_PASSWORD` и
   `TM_PROXY_CONTROL_TOKEN` — `openssl rand -hex 32` (токен обязан отличаться от
   `PROXY_FILTER_CONTROL_TOKEN` общего .env). Пул Decodo — только файлом
   `TRANSFERMARKT_PROXY_POOL_FILE`, сам JSON в env не кладётся.
4. Автомат: `install -d /usr/local/libexec/transfermarkt` и
   `install -m 0755 deploy/transfermarkt/{auto_deliver.sh,rotate_state.sh} /usr/local/libexec/transfermarkt/`,
   `install -m 0644 deploy/transfermarkt/env.sh /usr/local/libexec/transfermarkt/`.
5. Первое дерево: `bash deploy/transfermarkt/freeze_release.sh <master sha>`.

## Переезд (шаг 2, только по слову владельца, окно 11:00–03:00 UTC)

1. Проверка: в общей метабазе нет `running` рана TM; env-файл заполнен.
2. Пауза ingest и discover на общем планировщике; подтвердить SELECT `dag.is_paused`.
3. `mv` из `/root/data-platform-football/logs/` в `/root/transfermarkt-runtime/logs/`:
   `transfermarkt-approvals`, `transfermarkt-registry`, `transfermarkt-native-v2/cycles`;
   кэш страниц и леджер шлюза начинаются чистыми; `chown -R 50000 logs`.
4. Подъём:
   `docker compose -p transfermarkt-airflow -f <RELEASE>/deploy/transfermarkt/airflow.compose.yaml --env-file /root/data-platform-football/.env --env-file /etc/data-platform/transfermarkt.env up -d airflow-metadb airflow-init`,
   затем `bash <RELEASE>/deploy/transfermarkt/deploy.sh <RELEASE>` и
   `bash <RELEASE>/deploy/transfermarkt/postdeploy_checks.sh`.
5. Cron (копия `crontab -l` в `.prev-<дата>` до правки):
   `*/5 * * * * /usr/local/libexec/transfermarkt/auto_deliver.sh >> /root/watchdog/transfermarkt_auto_deliver_cron.log 2>&1`
   и `20 10 * * * /usr/local/libexec/transfermarkt/rotate_state.sh`;
   записать `<sha>` в `/root/transfermarkt-runtime/auto-deliver/transfermarkt-accepted`.

## Эксплуатация

- `deploy.sh <NEW> [<OLD>]`: коды 2 предпосылки, 4 контур занят (running-ран TM) или
  замок выката занят — бой не тронут, 5 шлюз, 6 импорт/приёмка, 7 паузы. Журнал —
  `/root/transfermarkt-runtime/deploy.log`. Откат = `deploy.sh` старого дерева.
- Автомат: окно 01:00–03:00 UTC, доставляет `origin/master`, при провале откатывает;
  три провальные ночи подряд — выключатель `auto-deliver/transfermarkt-auto-deliver.off`.
  `auto_deliver.sh --drill-rollback` — учения отката на копии принятого дерева.
- `rotate_state.sh --dry-run` — что удалила бы ротация.
