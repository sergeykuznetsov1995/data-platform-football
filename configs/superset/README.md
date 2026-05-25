# Superset — declarative datasets & dashboards

Все Superset-объекты в этом проекте версионированы как код. Никаких ручных
правок через Web UI — всё «как код» в `configs/superset/`.

## Структура

```
configs/superset/
  bootstrap.sh             # one-shot: admin + DB + datasets + dashboards
  superset_config.py       # Flask config (override secrets/CORS/...)
  datasources.yaml         # source-of-truth для database + datasets
  import_datasources.py    # idempotent upsert datasources.yaml -> Superset DB
  import_dashboards.py     # idempotent upsert dashboards/*.py -> Superset DB
  dashboards/
    __init__.py
    scouting_radar.py      # mart_scouting_radar
    referee_dashboard.py   # mart_referee_dashboard
    event_heatmap.py       # mart_event_heatmap
```

## Auth (env vars)

`bootstrap.sh` и оба импортёра ожидают переменные окружения (`.env`):

| Переменная                  | Назначение                                          |
| --------------------------- | --------------------------------------------------- |
| `SUPERSET_ADMIN_PASSWORD`   | пароль для admin (создаётся при первом bootstrap)   |
| `TRINO_SUPERSET_PASSWORD`   | пароль Trino-юзера `superset` (PASSWORD auth)       |
| `SUPERSET_SECRET_KEY`       | Flask secret (32+ байт)                             |

`bootstrap.sh` запускается из контейнера; импортёры — тоже (`docker compose
exec superset python ...`). Никаких HTTP-вызовов наружу — всё через Superset
SDK (SQLAlchemy session).

## Workflow

```bash
# Первый старт (создаёт admin + DB + datasets + dashboards):
make up-bi
make superset-init          # вызывает bootstrap.sh

# Изменился datasources.yaml → переимпорт datasets:
make superset-import

# Изменился любой dashboards/*.py → переимпорт dashboards:
make superset-dashboards
```

Оба импортёра идемпотентны:
- datasets ищутся по `(database_id, schema, table_name)` — обновляются на месте
- dashboards ищутся по `slug` — обновляются на месте; slices пересоздаются по
  имени `<dashboard_slug>__<slice_name>`

## Как добавить новый dataset

1. Дописать запись в `databases[0].tables` в `datasources.yaml`:
   ```yaml
   - table_name: <gold_table>
     schema: gold
     description: |
       Что таблица содержит, для чего используется.
     cache_timeout: 3600
   ```
2. Запустить `make superset-import`.
3. В Web UI Superset (Settings → Datasets) появится запись; метрики/колонки
   Superset auto-discover при первом запросе.

## Как добавить новый dashboard

1. Создать `configs/superset/dashboards/<name>.py` по образцу
   `scouting_radar.py`. Обязательные поля переменной `DASHBOARD`:
   - `slug` (уникальный, lowercase_with_underscores)
   - `title` (русский, человекочитаемый)
   - `description`
   - `datasets` (список `table_name` из `datasources.yaml`)
   - `charts` (список dict с ключами: `slice_name`, `viz_type`, `dataset`,
     `params`)
2. Запустить `make superset-dashboards`.
3. Дашборд появится в Web UI: http://localhost:8088/dashboard/list/

## Принципы UI/UX

- Заголовки и оси — на русском, с единицами измерения.
- Топ-N: сортировка DESC, лимит 10–25.
- Тренды: line chart (`viz_type: line`), x_axis = `match_date`.
- Распределения: histogram (20–30 bins) или bar chart.
- Pie charts с >5 секторами — запрещены; 3D-эффекты — запрещены.
- Цветовая семантика: победа=зелёный, поражение=красный, нейтральное=синий.

## Алерты

Алерты по метрикам реализованы НЕ через Superset Alerts, а через DAG
`dag_superset_alerts.py` (раз в 15 мин дёргает Superset REST API). Конфиг —
Airflow Variable `superset_alerts_config` (JSON). Подробности — в
`dags/utils/alerts.py`.
