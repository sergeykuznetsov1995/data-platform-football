# Карта источников и действующих инструкций

Начало работы: [AGENTS.md](../../AGENTS.md) → задача/issue и актуальный host handoff →
строка источника ниже → [проверки](../../TESTING.md) и [стадии сдачи](READINESS.md).
Карта показывает точки входа в код этой версии; она не утверждает, что DAG сейчас
включён, доставлен или принят. Расписания, mounts, окна доставки, PID и разрешения
проверяются на хосте по свежему handoff и паспорту источника.

| Источник | Точка входа в сбор | Контракт / описание данных | Runbook |
| --- | --- | --- | --- |
| ESPN | [current](../../deploy/espn/dags/dag_espn_current.py), [history](../../deploy/espn/dags/dag_espn_history.py) | [parser contracts](../../scrapers/espn/parser_contracts.py), [реестр/transport](../../configs/espn/README.md) | [новый контур, Bronze, доставка и приёмка](../../deploy/espn/README.md) |
| FBref | [ingest](../../dags/dag_ingest_fbref.py), [backfill](../../dags/dag_backfill_fbref.py) | [typed Bronze](../../scrapers/fbref/typed_bronze.py) | [шестичасовые окна](fbref-six-hour-windows-1322.md), [maintenance lock](fbref-maintenance-lock-1322.md), [транспорт](fbref-paid-transport.md), [Silver retirement](fbref-silver-retirement-1634.md) |
| SofaScore | [актуалка](../../dags/dag_refresh_sofascore_all_mens.py), [история](../../dags/dag_backfill_sofascore_all_mens.py), [ingest](../../dags/dag_ingest_sofascore.py) | [manifest](../../scrapers/sofascore/manifest.py), [конфигурация](../../configs/sofascore/README.md) | [production и приёмка](sofascore-production.md) |
| WhoScored | [ingest](../../dags/dag_ingest_whoscored.py), [backfill](../../dags/dag_backfill_whoscored.py) | [runtime contract](../../scrapers/whoscored/runtime_contract.py), [данные/DQ](whoscored-production.md) | [изолированный контур и автомат](../../deploy/whoscored/README.md); старые церемонии в production-документе сверять с этим runbook и host handoff |
| Transfermarkt | [ingest](../../dags/dag_ingest_transfermarkt.py), [discovery](../../dags/dag_discover_transfermarkt_registry.py) | [TableContract](../../dags/utils/transfermarkt_native_v2.py), [DQ contracts](../../dags/utils/transfermarkt_dq_contracts.py), [source-to-consumer checklist](../research/transfermarkt-native-v2-regression-checklist.md), [реестр](../../configs/transfermarkt/README.md) | [изолированный контур](../../deploy/transfermarkt/README.md) |
| FotMob | [orchestrator](../../dags/dag_orchestrate_fotmob.py), [daily trigger](../../dags/dag_trigger_fotmob_daily.py), [backfill](../../dags/dag_backfill_fotmob.py) | [catalog contract](../../scrapers/fotmob/catalog_contract.py) | [изолированный контур без церемонии](fotmob-isolated-ceremony-free.md); церемониальная доставка в `fotmob-production.md` не является текущим маршрутом этого контура |
| Understat | [ingest](../../dags/dag_ingest_understat.py) | [table contracts](../../scrapers/understat/contracts.py) | [сбор и приёмка](understat-production.md), [автомат доставки](../../deploy/understat/README.md) |
| ClubElo | [ingest](../../dags/dag_ingest_clubelo.py) | [parser](../../scrapers/clubelo/parse.py), [daily](../../scrapers/clubelo/daily.py) | [автомат и приёмка](../../deploy/clubelo/README.md) |
| Capology | [ingest](../../dags/dag_ingest_capology.py) | [scraper](../../scrapers/capology/scraper.py) | Отдельного актуального versioned runbook нет; до live-действий получить host passport/handoff |
| SoFIFA | [ingest](../../dags/dag_ingest_sofifa.py) | [scraper](../../scrapers/sofifa/scraper.py) | Отдельного актуального versioned runbook нет; до live-действий получить host passport/handoff |
| Football-Data / MatchHistory | [ingest](../../dags/dag_ingest_matchhistory.py) | [scraper](../../scrapers/matchhistory/scraper.py) | Отдельного актуального versioned runbook нет; до live-действий получить host passport/handoff |

Для ESPN `dags/dag_ingest_espn.py` — legacy, не вход нового контура. FotMob orchestrator
создаётся только для изолированного стека; не включай его в общем scheduler.
Наличие нескольких DAG у источника не разрешает запускать все или поднимать вторую
копию работающего процесса. Не выполняй команды установки из runbook только потому,
что они присутствуют в документе.

Общие модули writer требуют контракта всех потребителей и отдельного регламента
[shared-writer](../../deploy/shared_writer/README.md); исходная постановка и риск
доставки описаны в [плане ClubElo #1465](clubelo-1465-shared-writer-delivery.md).
Датированное разрешение/ограничение в старом плане не заменяет актуальный handoff.

Transforms: [Silver charter](../decisions/silver-charter.md),
[SQL-памятка](../../dags/sql/silver/README.md), [Gold-дизайн](../design/gold-star-schema.md),
[canonical naming](../decisions/canonical-naming.md). Они не добавляют Silver/Gold
к задаче, ограниченной Bronze. Историческая [матрица платформы](PROD-CHECKLIST.md)
не является свежей приёмкой источников.
