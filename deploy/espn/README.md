# deploy/espn

## `espn_stall_watch.py` — сторож простоя сбора ESPN (#1496)

Хостовый скрипт (cron `*/15`), не часть Airflow. Два правила, у каждого своя серия тревог в
Telegram:

| Правило | Когда тревога |
| --- | --- |
| `paused` | любой из `EXPECTED_DAGS` (`dag_ingest_espn`, `dag_trigger_espn_daily`, `dag_monitor_espn`, `dag_discover_espn_registry`) в metadb `espn-airflow-airflow-metadb-1` на паузе или отсутствует; metadb не отвечает — тревога того же правила «недоступен» |
| `stall` | в `espn_lineup_generation_v2` ∪ `espn_matchsheet_generation_v2` нет ни одного матча с `_source_fetched_at` за 36 ч (не `_ingested_at` — в этих таблицах оно = execution_date прогона); Trino недоступен — правило пропускается |

Серия: первая тревога → тишина, «⏳ продолжается N ч» раз в 24 ч → через 24 ч issue
(`source:espn,area:bronze,type:bug`, заголовок `ESPN: сторож [<правило>] — …`; открытая issue с
тем же заголовком не дублируется) + карточка Blocked → «✅ отбой», когда условие снято.
State — `/root/watchdog/state/espn_stall_state.json` (+ `.lock`), неподтверждённые Telegram-сообщения
ждут в `pending` и повторяются следующим тиком.

Контракт на #1504: реакция нового контура на сбой — красный турнир + тревога этого сторожа, никаких
`pause_all` / `on_failure → pause`.

### Установка на хост (после мержа)

```bash
cp deploy/espn/espn_stall_watch.py /root/watchdog/espn_stall_watch.py
chmod 755 /root/watchdog/espn_stall_watch.py
python3 -m py_compile /root/watchdog/espn_stall_watch.py
python3 /root/watchdog/espn_stall_watch.py --dry-run --state /tmp/espn-dry.json   # --dry-run без --state запрещён
crontab -l > /root/watchdog/crontab.prev-$(date +%Y%m%d)
( crontab -l; echo '*/15 * * * * /usr/bin/python3 /root/watchdog/espn_stall_watch.py >> /root/watchdog/espn_stall_watch.log 2>&1' ) | crontab -
```

Откат: `crontab /root/watchdog/crontab.prev-<дата>`; state можно удалить.

Ручной прогон: `--dry-run --state <отдельный файл> [--now 2026-09-25T22:00:00Z]` — печатает, что
отправил бы и какую issue завёл бы; Telegram и gh не трогает.

### Что менять дальше

- **#1503** (свой контур ESPN): `METADB` и `EXPECTED_DAGS` — на контейнер и DAG нового контура.
- **#1507** (автодоставка): `BRONZE_TABLES` — на новые таблицы bronze (#1156); если в них
  `_ingested_at` честное (время коммита), `TS_COL` можно оставить `_source_fetched_at` — правило
  «новые матчи за 36 ч» от этого не меняется.
