# Superset dashboards (declarative)

Дашборды Superset для football-платформы определены **как код** — Python-скрипты,
которые создают объекты (`Dashboard`, `Slice`) через нативный SDK Superset.
Это даёт идемпотентность, версионирование в git и работу без ручных шагов в UI.

## Зачем declarative подход

Альтернативы и почему мы выбрали Python-скрипты:

| Подход | Плюсы | Минусы |
|--------|-------|--------|
| Сборка в UI + экспорт ZIP | Удобно для аналитика | ZIP бинарный, плохо diff-ится в git, при пересборке Superset DB всё теряется |
| YAML-импорт через CLI | Декларативно | Superset 4.x требует ZIP со специфической структурой (`metadata.yaml` + `databases/` + `datasets/` + `charts/` + `dashboards/`); собирать вручную — много шаблонного кода |
| **Python-скрипт через SDK** | Идемпотентно, читабельно, легко добавлять новые виджеты | Зависим от внутренних моделей Superset (`Slice`, `Dashboard`, `SqlaTable`) |

Мы выбрали третий вариант — он соответствует подходу `import_datasources.py` (Wave 2)
и работает без сборки промежуточных ZIP.

## Текущие дашборды

| Slug | Title | Чартов | Источники |
|------|-------|--------|-----------|
| `player-overview` | Player overview | — | `fct_player_match`, `fct_shot`, `fct_match_rating`, `dim_*` |
| `league-overview` | Обзор лиги + игроки | 21 | `fct_standings`, `fct_team_season_stats`, `fct_team_elo`, `fct_player_season_stats`, `fct_player_salary`, `fct_player_market_value`, `dim_*`, `silver.transfermarkt_players` |

(Дашборды производного gold-этажа — team-form-overview, match-outcomes,
scouting-radar, referee-dashboard, event-heatmap — удалены в epic #478.)

## Как добавить новый дашборд

1. Создай файл `<short_name>.py` в этой директории. Он должен:
   - определять функцию `create_dashboard()` без аргументов;
   - проверять идемпотентно `slug` и выходить, если дашборд уже есть;
   - использовать утилиты `_find_table` / `_make_slice` (см. `player_overview.py`).
2. Добавь имя модуля (без `.py`) в список `DASHBOARDS` в `import_dashboards.py`.
3. Применить:
   ```bash
   make superset-init     # полный bootstrap
   # либо вручную внутри контейнера:
   docker compose exec superset python /app/pythonpath/dashboards/import_dashboards.py
   ```

### Шаблон скрипта дашборда

```python
from superset import db
from superset.app import create_app
from superset.connectors.sqla.models import SqlaTable
from superset.models.core import Database
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice

DASHBOARD_SLUG = "my-new-dashboard"

def create_dashboard():
    app = create_app()
    with app.app_context():
        if db.session.query(Dashboard).filter_by(slug=DASHBOARD_SLUG).one_or_none():
            return
        database = db.session.query(Database).filter_by(database_name="trino_iceberg").one()
        # ... build slices ...
        dashboard = Dashboard(dashboard_title="...", slug=DASHBOARD_SLUG, slices=[...])
        db.session.add(dashboard)
        db.session.commit()
```

## Альтернатива: ZIP-экспорт из UI

Иногда проще собрать сложный layout в UI и положить ZIP сюда. `bootstrap.sh`
после Python-импорта проходит по всем `*.zip` в этой директории и импортит их
через `superset import-dashboards`. Используй для одноразовых сложных дашбордов
(где нужно чарты позиционировать вручную) или как backup перед миграцией.

## Локальная проверка

```bash
make up-bi              # поднять Superset stack
make superset-init      # bootstrap: db upgrade + admin + datasources + dashboards
# открыть http://localhost:8088, login admin / $SUPERSET_ADMIN_PASSWORD
# в меню Dashboards убедиться что "Team form overview" и "Match outcomes" есть
```

При повторном запуске `make superset-init` импортёр пишет
`dashboard '<slug>' already exists; skipping` — это ожидаемое поведение,
ручные правки в UI не затираются.

## Замечания по `viz_type` params

Параметры `params` для каждого `viz_type` берём из публичной документации
Apache Superset 4.x и из примеров `superset/examples/`. Минимальный набор:

| `viz_type` | Обязательные params |
|------------|---------------------|
| `dist_bar` | `metrics`, `groupby` |
| `echarts_timeseries_line` | `x_axis`, `metrics`, `groupby` (опционально) |
| `pie` | `metric`, `groupby` |
| `heatmap` | `all_columns_x`, `all_columns_y`, `metric` |
| `table` | `all_columns` или `metrics`+`groupby` |
| `big_number_total` | `metric` |

Поле `datasource` (`"<id>__table"`) и `viz_type` подставляются в `_make_slice`
автоматически — в скрипте можно их не указывать.
