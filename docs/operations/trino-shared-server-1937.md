# Общий Trino под 8 парсерами — память, аккаунты на источник, перезапуск (#1937)

Решения гриля 29.10.2026 — `docs/legacy-root/TRINO-GRILL-2026-10-29.md` (канон).

## Если по-простому

Один Trino обслуживает все источники. С 5 ГБ heap он падал по памяти шесть раз за 25–28.10
(MERGE SofaScore по таблицам из ~130 тыс. файлов). Теперь: контейнер 20 ГБ без swap, heap 10 ГБ,
3 ГБ отведено под память планирования (там и жили OOM), история запросов короткая (#1620).
Каждый источник получает свой логин `src_<источник>` и свою группу ресурсов. Что это даёт и чего
нет: группа ресурсов — мягкий потолок (`softMemoryLimit`): когда запросы источника суммарно
держат больше 40 % памяти запросов, его новые запросы ждут в очереди, а не стартуют поверх
остальных; уже идущий запрос она не убивает. Жёсткий потолок на один запрос — общий
`query.max-memory-per-node=6GB` (запрос падает с `EXCEEDED_LOCAL_MEMORY_LIMIT`). Ни то, ни
другое не ограничивает неучитываемую память планирования (манифесты Iceberg) — её закрывают
headroom 3 ГБ и запрет MERGE без фильтра партиций (гриль, п.5). В `system.runtime.queries`
становится видно, чей запрос.

## Что где

| Что | Файл |
|---|---|
| Лимит контейнера 20G / memswap 20G | `compose.yaml` → `trino.deploy.resources.limits`, `memswap_limit` |
| Heap 10g | `configs/trino/jvm.config` |
| query 6GB, headroom 3GB, #1620 | `configs/trino/config.properties` |
| Группы ресурсов `sources.${USER}` | `configs/trino/resource-groups.json` (нужен рестарт) |
| Права восьми `src_<источник>` = как `airflow` | `configs/trino/rules.json` (hot-reload 30 с) |
| Аккаунты `src_<источник>` | `configs/trino/password.db` (gitignored) ← `scripts/trino/sync_source_users.sh` |
| Пароли | `.env`: `TRINO_SRC_<ИСТОЧНИК>_PASSWORD` (шаблон в `.env.example`) |

## Выкатка (ночное окно, общий стек — только по SHARED-STACK-PROTOCOL)

Живой `trino` принадлежит compose-проекту из трёх файлов прод-дерева (`/root/dpf-whoscored-merge`);
его `configs/trino` смонтирован в контейнер. Поэтому:

1. Перенести этот PR в прод-дерево (advance прод-дерева или перезапись файлов на месте —
   `open(path, "w")`, без rename, см. memory `dpf-prod-tree-inplace-patch-vs-attestor`).
2. В `.env` прод-дерева добавить `TRINO_SRC_*_PASSWORD`, затем
   `scripts/trino/sync_source_users.sh configs/trino/password.db .env`.
3. Лимит cgroup можно поднять без простоя заранее (сделано 29.10 10:50 МСК):
   `docker update --memory 20g --memory-swap 20g trino`.
4. Heap и группы ресурсов требуют рестарта JVM — только свой сервис, только ночью:
   ```bash
   docker compose -f /root/dpf-whoscored-merge/compose.yaml \
     -f /root/whoscored-954-runtime/deploy-949/prod.override.yaml \
     -f /root/stack-recovery/recovery.override.yaml \
     --project-directory /root/dpf-whoscored-merge up -d --no-deps trino
   ```
5. Проверка: `docker inspect trino` → Memory=21474836480, `RestartCount` не растёт;
   `SELECT * FROM system.runtime.nodes`; `curl -k https://localhost:8443/v1/info`;
   в логе `======== SERVER STARTED ========` без `OutOfMemoryError`.

## Перевод источника на свой логин (по одному, отдельные PR)

В контуре источника задать `TRINO_USER=src_<источник>` и `TRINO_PASSWORD=$TRINO_SRC_<ИСТОЧНИК>_PASSWORD`
(переменные, которые читает его `trino_manager`/`get_trino_connection`), перезапустить только его
scheduler/шлюз, убедиться по `system.runtime.queries` (`user = 'src_<источник>'`). Пока источник не
переведён, он работает под `airflow` (группа `machine`) — ничего не ломается.

## Критерий «сдано» (гриль, п.6)

14 дней `RestartCount` не растёт + RSS < 80 % лимита + `SELECT count(*)` по самой тяжёлой
bronze-таблице ≤ 10 с. Смотрит утренний сторож (строка «Trino»).
