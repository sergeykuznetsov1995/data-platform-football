# FBref — сводное ревью 24 августа — 12 сентября 2026

**Bronze FBref ещё не готов к production.** Сбор продолжает записывать данные, но ни один из 19 завершённых плановых current-запусков в проверенном окне не завершился успешно. История остановилась 06.09 в 01:21:47 UTC. Полнота, соблюдение current ≤24h и устойчивая одновременная работа не приняты.

Согласованный scope — [Raw/generic/typed Bronze](agreed-scope.md); состояние Silver/Gold не является критерием этого GO. Подготовка реализации остановлена пользователем. Этот отчёт фиксирует результат ревью для дальнейшего обсуждения.

## Подготовленные изменения

| Дефект | Исправление и подтверждение | Что ещё не доказано |
|---|---|---|
| P0: повторяющиеся HTML table IDs останавливают history recovery | У Atlas 2009–2010 две DOM-таблицы с одинаковым ID, но 5/6 строками. Первый manifest сохраняет старый ключ; следующие получают детерминированный `table_instance_id`. Реальный сохранённый raw воспроизводит ошибку на исходном коде; исправление проходит повторный replay и 8 дополнительных вариантов. | Доставка и replay source observations не выполнены. У ранее успешных equal-count duplicates могла потеряться отдельная manifest identity; нужен адресный аудит. |
| P1: `against` или соседняя stat category выдаётся за `for` | Удалён substring fallback; сохранены поддержанные точные source/legacy IDs. Два новых теста сначала упали; после исправления finder/parser suite: 228 passed. | Наличие загрязнённых production typed rows не установлено. Исправление будущего разбора не ремонтирует старые строки. |
| P1: новая очередь скрывает настоящую просрочку | Вместо `stale - never_fetched` SQL считает непересекающиеся overdue fetched и overdue never-fetched; consumer проверяет сумму и обрабатывает legacy payload консервативно. Пять регрессий сначала упали; затем 239 focused tests passed. | Это дефект промежуточного verdict. Отдельный финальный gate оставался; публикация просроченных данных этим багом не доказана. |
| Гонка SIGTERM readiness в тесте | `READY` перенесён внутрь `try/finally`; детерминированное воспроизведение с задержкой проверяет finalizers. | Production signal handler не менялся. |

Итог offline-проверок подготовленного набора: **1770 passed, 1 skipped**, 60 файлов тестов, 51,95 секунды. Пропущен реальный Airflow import, поскольку среда не содержала Airflow. Ruff, whitespace check и generated runtime evidence checks прошли. [Сохранённый лог](unit-verified.log) получен **до позднейшей подготовки audit utility**, поэтому не является её проверкой. Код не доставлен; live writer concurrency и исправление старых production rows не подтверждены.

Изменения затрагивают `scrapers/fbref/pipeline.py`, `scrapers/fbref/parsers/finders.py`, `scrapers/fbref/control/store.py`, `dags/utils/fbref_pipeline_tasks.py` и регрессионные тесты. Согласованные generated evidence и source attestation тоже были обновлены; будущий релиз должен идентифицировать полный проверенный набор. Самих патчей в этом архиве нет.

## Наблюдённая эксплуатация

| Контур | Успешно | Неуспешно |
|---|---:|---:|
| Плановый current | 0 | 19 |
| Ручной current | 1 непубликующая canary | 2 |
| История | 40 | 15 |
| Bootstrap | 3 | 1 |

На момент среза ещё один current выполнялся. 18 из 21 failed current и 10 из 15 failed history использовали запросы; часть Raw/Bronze могла сохраниться. Расход запросов не доказывает завершённую обработку. Ежедневный FBref generic-stage janitor упал все 20 проверенных дней, 60 попыток; 10 retained stages требуют разбора. Другая проверенная high-churn maintenance работала все 20 дней.

По [runtime audit](runtime-audit.md), current failures включали 8 freshness gates, 2 lock collisions с историей, 3 subprocess timeouts, 1 SIGTERM, 3 конечных HTTP ошибки, 1 oversize, 2 transport/metering failures и 1 clearance exhaustion. У истории: 6 timeouts, 3 transport/metering, 2 clearance и 4 manifest collisions. Последние три history runs повторяли одну ошибку в recovery без сетевых запросов.

## Незавершённые требования

1. **Current/history фактически сериализованы.** Оба держат source publication lock на весь проход; `publish=false` у истории его не отменяет. 2–3 сентября история перекрыла запуск current в 06:00 UTC. Общий Airflow pool имеет один слот, поэтому одного изменения lock недостаточно. Необходима проверка writer fencing, admission и реального одновременного выполнения с резервом current.
2. **Оценка окна не ограничивает длительность.** Минимальный интервал запросов не задаёт верхнюю границу parse/persist/recovery. Driver ожидал 180 минут, фактические волны занимали 4–6 часов. Абсолютный deadline и сохраняемый partial outcome пока не реализованы.
3. **Current scope содержит неподходящие исторические сезоны и реальную просрочку.** Последние сезоны прекратившихся соревнований 68/76/79 считаются current и quarantined с `schedule_link_missing`. Одновременно просрочены действующие schedules/season_stats. Ослабление DQ не разрешает обе причины.
4. **Generic полнота не означает typed полноту.** Standings/squad/player/matchlog проходят table/cell parser, но отдельная domain typed promotion неполна. Внетабличные данные остаются в HTML; `against` требует собственной perspective. У исключённых stat routes нет периодической availability-перепроверки.
5. **Oversized discovery pages могут скрыть целое поддерево.** Одиночный oversize season/schedule получает terminal outcome; обычный обход его не переоткрывает. Успешные соседние страницы не доказывают полноту сезона.
6. **Работающий релиз не идентифицирован одним Git SHA.** Runtime читает изменённое интеграционное дерево. Нужны проверяемые effective source/image fingerprints и согласованная доставка. Отдельно current DAG ждёт Silver child перед release lock; самостоятельный Bronze outcome ещё не реализован.

## Полнота и точечная cleanup

Registry содержит 117 мужских соревнований и 1 975 сезонов. Известный historical frontier: 185 184 targets = 14 476 fetched + 170 385 pending/retry + 323 quarantined. Discovery расширяет знаменатель: 7,82% fetched — доля известной очереди, **не процент всей истории FBref** и не основание для ETA.

Current male scope: 226 012 targets, 75 197 вне SLA. Более узкая publication population: 12 069 targets, 501 вне SLA, 82 never fetched. Отдельный backlog завершённых current-матчей — 11. Это разные популяции. Несколько SELECT выполнялись при живом ingest; числа не образуют единый immutable snapshot.

12.09 в 10:29:25 UTC после проверки зависимостей и транзакционного dry-run обратимо архивирована **ровно одна доказанная тестовая строка** `fbref:test:concurrent-index:26816b472c144534a6e4348964e56687`. Она не имела fetch attempts и создавала одну ложную просрочку. Её failed immutable cohort сохранён, настоящий `fbref:competition_index:all` и другой run с реальным матчем не изменены. Числа выше относятся к срезу **до** этой операции. Backup и apply evidence сохранены локально, не опубликованы.

## Доска и продолжение

[Аудит доски](board-audit.md) зафиксировал 23 открытые FBref issues: 15 Todo, 5 In Progress, 2 Blocked, 1 In Review. Ни одна не обновлялась после 20 августа. Пять PR 25–26 августа имеют зелёный финальный CI; шесть исторических CI failures уже исправлены. Открытая карточка сама по себе не доказывает действующий дефект.

20 из этих 23 карточек содержат Bronze-работу; #870/#903/#916 преимущественно downstream. Это классификация исторического среза, не итог нового board reset. Архивный [проект плана](prior-plan-draft.md) и [downstream backlog](downstream-backlog.md) остаются входом для следующих ревью. Семидневный soak согласован и **не начат**; завершение всей истории до Bronze GO не требуется.

Аудит не выполнял полный row scan крупных Bronze-таблиц и не сертифицирует все datasets/исторические сезоны. Timestamps snapshots доказывают запись, но не полноту. Production typed contamination и потеря исторических строк из-за коллизии сокращённых обозначений сезонов разных веков не установлены.
