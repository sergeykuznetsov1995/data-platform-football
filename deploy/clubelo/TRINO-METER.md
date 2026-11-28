# Счётчик Trino ClubElo (#2132)

Измеряет по каждому UTC-дню, соблюдает ли ClubElo норму записи Trino и ходят ли его
запросы от `src_clubelo`. Результат — строка «Trino clubelo <день> PASS …» в утренней
сводке. Приёмка задачи — три полных дня подряд с PASS.

## Состав на хосте

| Что | Где | Из репозитория |
| --- | --- | --- |
| Измеритель | `/root/watchdog/clubelo_trino_meter/trino_load_meter.py` | `deploy/clubelo/trino_load_meter.py` |
| Хранилище и отчёты | `/root/watchdog/clubelo_trino_meter/{clubelo.sqlite3,daily/}` | — |
| Суточный агрегат | `/etc/cron.d/clubelo-trino-meter` (04:35 MSK) | — |
| Приёмник событий | `fotmob-meter-receiver.service`, `/root/watchdog/fotmob_meter_b/` | `scrapers/fotmob/load_meter_receiver.py`, `scrapers/fotmob/load_meter.py` |
| Минутный heartbeat | cron → `load_meter_b_cron.sh --clubelo-root …` | — |
| Строка сводки | `morning_report.py`, `clubelo_section` | `deploy/clubelo/morning-report-2132.patch` |
| Чтения сторожей | `/root/watchdog/clubelo_trino_ro.sh` | `deploy/clubelo/clubelo_trino_ro.sh`, `*-2132-source-identity.patch` |

Приёмник общий с FotMob, поэтому вторая копия `trino_load_meter.py` лежит рядом с
ним как sibling-модуль. Обёртка сторожей требует выпуск клиента мониторинга с
`--identity` ([клиент мониторинга](../../docs/operations/trino-monitor-client.md)).
Автодоставка ClubElo эти пути не доставляет: установка и обновление — вручную по
разрешению владельца, с резервными копиями в `/root/clubelo-deliveries/`.

## Что считается

- Скан — физическая запись входа запроса Trino, коммит — снимок Iceberg по `committed_at`.
- Актёры: `source` (`src_clubelo`), `monitor` (переданный `--clubelo-monitor-user`),
  `airflow`, `foreign`, `unknown`.
- Staging-таблица `…__stg_<12 hex>` — не таблица Bronze: `CREATE … WHERE false`, `INSERT`,
  `SELECT` и `DROP` вокруг неё и `DESCRIBE` не создают активности по таблицам источника.
- `ALTER TABLE … EXECUTE optimize|expire_snapshots|remove_orphan_files` от `airflow` —
  обслуживание платформы, а не `airflow_table_use`.
- Снимки делятся по движку. Коммит Trino (`trino_user`, `trino_query_id`) сопоставляется
  с успешным запросом записи по query id. Снимки bulk-writer (`dpf.operation` и их
  delete-половины) — коммиты каталога: в снимке нет принципала. Всё остальное даёт
  `unattributed_snapshot_or_output` или `non_source_output`.
- Чтение таблиц ClubElo от общей учётки `airflow` даёт `unattributed_airflow_read`, день
  не PASS. Поэтому сторожа ClubElo (часовой сторож простоя и секция утренней сводки)
  читают через `clubelo_trino_ro.sh`, то есть от `src_clubelo`.

## Обновление

1. Сверить SHA-256 установленных копий с ожидаемыми прежними.
2. Заменить файлы через временный файл и `rename`; прежние байты сохранить в
   `/root/clubelo-deliveries/<ревизия>/`.
3. `systemctl restart fotmob-meter-receiver.service`, проверить `/healthz` и
   `/clubelo-healthz`.
4. Дождаться одной полной минуты: в `clubelo.sqlite3` появляется строка `minutes` с `ok=1`.
5. Утром проверить строку за вчера:
   `trino_load_meter.py render --root /root/watchdog/clubelo_trino_meter`.

## Откат

Вернуть сохранённые байты в обратном порядке и перезапустить приёмник. Хранилище и
отчёты обновление не меняет.
