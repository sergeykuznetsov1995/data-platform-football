# WhoScored — production rollout

Статус: parser v8, full-history discovery и production write-smoke прошли.
World Cup replay после исправления HTTP 502 успешен. Следующий 6h canary прошёл
throughput gate, но остановился на настоящем Cloudflare-блоке EPL. Безопасный
общий темп 546 ms и persistent source circuit 15/30/60 минут реализованы,
прошли isolated suite и независимый review `READY`. Точный extension уже
развёрнут в здоровом FlareSolverr; первый live EPL replay и последующие
level-1/level-2 half-open пробы встретили Cloudflare и правильно остановились
fail-closed без paid traffic. Источник/IP остаётся заблокирован после пауз 15,
30 и 60 минут; circuit держит максимальный 60-минутный cooldown. До полного 6h
canary с exit 0 source pool остаётся 2, DAG-и paused, а `all_catalog` не
создаётся. Этот файл фиксирует фактическое состояние, а не заменяет evidence
из Airflow/Trino.

## Утверждённый scope

- Источник: полный доступный каталог мужских senior-турниров WhoScored.
- Daily и backfill: только direct transport, `paid_proxy_bytes = 0`.
- Результат: append-only raw и 25 business datasets в Iceberg; current views
  должны читаться через существующий Trino/DataGrip endpoint.
- `proxy_filter` получает пул только из `PROXY_POOL_JSON`, file fallback
  выключен. Legacy `proxys.txt` остаётся смонтированным в Airflow для других
  источников, но не монтируется в `proxy_filter`.
- SeaweedFS остаётся в текущем режиме `weed mini`. Backup DAG остаётся paused;
  off-host/WORM backup и topology cutover исключены из этой активации по
  решению владельца. Принят риск потери raw/Iceberg при отказе единственного
  хоста; это не должно блокировать корректность записи WhoScored.

## Обязательные gate-ы

1. Isolated WhoScored suite, Ruff/compile, Compose render и настоящий
   `airflow dags list-import-errors` проходят без ошибок.
2. Runtime preflight подтверждает parser `whoscored-parser-v8`, report schema 3,
   25 datasets, интерфейс `IcebergWriter.bulk_arrow` и SHA-256 критичных файлов.
3. DAG-и paused; выполнены additive schema init и full-history discovery.
4. Production write-smoke на точном active scope из закреплённого каталога
   создаёт parser-v8 manifests, `discovery_mode` доступен,
   physical/manifest/current counts совпадают, `paid_proxy_bytes = 0`; те же
   current views читаются через Trino/DataGrip. На дату активации canonical
   scope — `ENG-Premier League=2627`; `2526` уже historical и не принимается
   командой `daily`.
5. Непубликующий 4-worker canary работает не менее 6 часов на historical
   match/preview/profile/multi-stage workload: не менее 144,000 page units/day,
   RSS не выше 12 GiB, без OOM/restart и paid traffic.
6. Только после green canary установить:
   `WHOSCORED_SOURCE_POOL_SLOTS=4` и
   `WHOSCORED_BACKFILL_ASSUMED_REQUEST_UNITS_PER_DAY=144000`. Физический hard
   ceiling выводится кодом: `4 * 30 * 1440 = 172800`.
7. Один manual daily, затем один scheduled daily проходят все DQ/SLO gate-ы.
8. Создаётся ровно один immutable `all_catalog` plan; завершение не позднее 30
   дней, без schedulable work, с historical DQ всех 25 datasets.

Если 4-worker canary не проходит, production остаётся на двух source slots и
полный `all_catalog` backfill не запускается. Нельзя повышать planning value
без одновременного реального pool limit и доказанного canary throughput.

## Выполнено / осталось (2026-07-14/15)

- Runtime развёрнут с candidate HEAD
  `87da72f59c92ae7cbe9b3ba83d3a7947c7975ae3`; runtime contract code-tree
  SHA-256 — `3426b6a3cb4eae2f8a25568ae80c53b177265543a89c12b15a2b17a53668e577`.
  DAG import errors отсутствуют, direct/DQ pools — `2/2`, все три WhoScored
  DAG-а paused.
- Full-history discovery завершён успешно: report
  `/opt/airflow/logs/whoscored/manual/20260714T152100Z/discovery.json`, parser
  `whoscored-parser-v8`, 31,963 rows, 433 competitions, 7,477 seasons, 15,979
  stages, 7,472 eligible scopes, quarantined=0, errors=0, paid bytes=0. Catalog
  batch —
  `wsc2-c1d277275b85c0064ee72819d37a58a1b86a5852a5e9aea4636e3ec82ae4cfd5`;
  manifest, physical tables и загруженный catalog имеют одинаковые counts и
  payload/schema SHA-256
  `23fed917903ac69c9fe08d67982f1234b5240018853ab5073e71961fc6971a93`.
- Первая попытка v8 discovery
  (`.../20260714T151900Z/discovery.json`) fail-closed завершилась до source
  traffic на v7 baseline. Только explicit `--full-history` теперь может
  использовать строго валидный v7 catalog как baseline для loss detection;
  обычные reads и новые manifests остаются v8-only.
- Первоначальный smoke selector `ENG-Premier League=2526` fail-closed отклонён
  до source traffic как inactive historical scope, paid bytes=0. Фактический
  active-scope smoke `ENG-Premier League=2627` сохранён в
  `/opt/airflow/logs/whoscored/manual/20260714T161016Z/daily_epl_2627.json`:
  schema 3, parser v8 scope manifest
  `wss2-306d1e033e7a161427c5a542516664d6011ab7316943e54ace2e93f54478a249`,
  status=success, 380 schedule rows, 144 bet rows, errors=0, paid bytes=0.
  `validate_scope_result` прошёл с точным catalog batch: feed states 68/68,
  duplicate/missing/parity counters равны нулю. Current views читаются через
  Trino/DataGrip-facing слой.
- После smoke SeaweedFS и Trino не пересоздавались: container identity не
  изменился, restart=0, OOM=false. `proxy_filter` healthy, file fallback=false,
  legacy mount отсутствует, `daily_total_bytes=0`. Backup DAG остаётся paused;
  storage topology не менялась.
- Первый 6h canary artifact
  `/root/fbref-949-runtime/whoscored-capacity-20260714T161700Z.json` корректно
  fail-closed остановился через 5.792 s: host venv не содержал production pin
  `curl_cffi==0.15.0`. Bronze/DDL writes и paid traffic отсутствовали. Harness
  теперь проверяет dependency до workers и связывает venv/version с runtime
  identity; urgent delta прошёл 64 tests и независимый review без
  Critical/Important замечаний.
- Повторный create-once canary
  `/root/fbref-949-runtime/whoscored-capacity-20260714T164334Z.json` корректно
  fail-closed остановился через 1,032.838 s. Два EPL worker-а завершили шесть
  workflow runs и 546 page units; World Cup worker остановился на preview
  `1976987`, где источник явно не отдаёт полный набор preview-структур. Paid
  bytes/routes равны нулю, memory/restart/OOM/runtime-identity gate-ы зелёные.
  Продакшен-валидация preview не ослаблялась.
- Capacity workflow теперь детерминированно проверяет не более девяти
  завершённых кандидатов и выбирает три с полным typed preview. Только явный
  `DatasetStatus.NOT_AVAILABLE` исключает кандидата; ошибка транспорта,
  парсера или drift типа fail-closed останавливает run. 69 workflow/capacity
  tests, Ruff и compile прошли; повторный независимый review не нашёл
  Critical/Important замечаний.
- Непубликующий World Cup rehearsal сохранён в
  `/root/fbref-949-runtime/whoscored-workflow-20260714T204118Z.json` (SHA-256
  `eae45ded16c6372ed3c38fbf9fc708c8832c1484cb95ffbb9770c5249f9edff2`). Он
  завершился success за 1,265.659 s: 13 стадий, schedule/matches/previews/
  profiles полностью успешны, cold=925 завершённых page units, warm=0 source
  requests, incremental=1 source request, paid bytes/routes=0. Выбранные
  матчи одинаковы во всех фазах: `1953853`, `1976989`, `1953860`.
- Попытка 6h canary
  `/root/fbref-949-runtime/whoscored-capacity-20260714T210456Z.json`
  завершилась fail-closed через 275.255 s: внешний rollout остановил и
  пересоздал `airflow-scheduler`. Это не результат проверки throughput.
  Page units=0, paid bytes/routes=0. После остановки девять browser sessions
  были удалены вручную.
- Старый 4-worker abort rehearsal
  `/root/fbref-949-runtime/whoscored-capacity-abort-rehearsal-20260714T214335Z.json`
  оставил две sessions. Это отрицательное evidence. После него добавлены
  owner-bound cleanup API, host lock, state mode 0600, 95-секундное quiet
  window, process-group/namespace cleanup, recovery после SIGKILL и
  fail-closed проверка FlareSolverr runtime.
- Финальная focused-проверка прошла: 403 tests. Независимые проверки:
  FlareSolverr — 175 tests/READY, capacity supervisor — 333 tests/READY.
  Исторический полный unit suite дал 5,723 passed, 46 skipped и одну
  SofaScore fingerprint-ошибку, которая позже была исправлена; это не
  исключение для релиза. Тест теперь называется:
  `tests/unit/scripts/test_bench_sofascore_paid_canary.py::`
  `test_shipped_candidate_has_exact_required_v3_classes_and_shapes`.
- Развёрнут FlareSolverr container
  `34de7c325464e6af7a2a1641107baf497efc0f78f6739464a2aad42dba61828a`,
  image
  `sha256:7962759d99d7e125e108e0f5e7f3cdbcd36161776d058d1d9b7153b92ef1af9e`,
  extension SHA-256
  `45ddcea7d36d4d91587ceb7ad04dff9aa75c182d74264c682f2b254ea501eb46`.
  Live 4-thread probe создал и заблокировал четыре requests; control API увидел
  active=4 и ответил за 0.005 s. Все requests освободились, owner и global
  sessions вернулись к нулю. Live log-redaction probe: `leak_count=0`.
- Recovery после parent SIGKILL подтверждён artifact
  `/root/fbref-949-runtime/whoscored-capacity-abort-recovery-20260714T233742Z.json`,
  SHA-256
  `137c180fe7980f3e8c8c69c06c7e1349d374c3fd23ccd80af43f06fc133ce970`.
  `preflight_required=true`, preflight/final zero verified, quiet window и два
  final scans прошли, state удалён, paid traffic=0. Общий status ожидаемо
  `failed`: короткий 10-секундный rehearsal не проверял throughput/workload.
- Обязательный 6h canary от `2026-07-14T23:42:35Z` сохранён в
  `/root/fbref-949-runtime/whoscored-capacity-20260714T234235Z.json` (SHA-256
  `872e60f0aca1c6dc6d0b7c4f946d74344347f38ae01f69eb55c004044ed22084`). Он
  завершился `failed` через 12,987.038 s работы (около 3 ч 36 мин;
  13,087.471 s всего): stop reason=`worker_health`, поэтому gate длительности
  тоже не пройден. Throughput gate прошёл: 178,906.757 page units/day,
  page units=26,892, завершено 112 runs; по workers — 46/10/46/10. Paid
  traffic=0; memory, containers, runtime identity, non-publishing и cleanup
  gate-ы зелёные. Cleanup подтвердил final zero и удалил owner state.
- Причина остановки: World Cup worker на iteration 10 получил partial schedule
  из-за typed HTTP 502. В browser batch validation retryable typed 502 ошибочно
  считался terminal. Исправление повторяет только упавший URL, сохраняет
  успешный cache, меняет session, не включает paid route и перед каждым
  физическим retry снова берёт rate token. Постоянный 502 остаётся fail-closed.
- Первый live diagnostic безопасно воспроизвёл `HTTP 502 rendered as HTTP 200`:
  source attempts=3, paid traffic=0, cleanup зелёный. Исправление получило
  independent review `READY`; прошли 86 transport tests и 483 combined focused
  tests, Ruff, compile и diff check.
- Исправленный World Cup replay завершился успешно:
  `/root/fbref-949-runtime/whoscored-workflow-http-backoff-replay-20260715T103454Z.json`,
  SHA-256
  `50f5cd78eb8230716c71ebe2f0c4dd0a9498882c777409d6beef7d66cb74c62b`;
  paid traffic=0, cleanup зелёный.
- Следующий 4-worker canary сохранён в
  `/root/fbref-949-runtime/whoscored-capacity-20260715T110327Z.json`, SHA-256
  `e57bb7b6a7b915415987da86d6d098d10ebe13ede93fe938b320501367ad21d0`.
  Он fail-closed остановился через 6,733.609 s на настоящем Cloudflare-блоке
  EPL после четырёх browser attempts. Throughput до блока был 172,527.745 page
  units/day; paid traffic, memory, runtime identity и cleanup gate-ы зелёные.
- EPL retry4 replay и два bounded diagnostics подтвердили, что blocked XHR
  возвращает Cloudflare 403 и даже navigation той же browser session получает
  typed CF. Evidence:
  `whoscored-workflow-epl-retry4-replay-20260715T133113Z.json` (`6583125a...`),
  `whoscored-epl-feed-diagnostic-20260715T134103Z.json` (`fa64e16c...`) и
  `whoscored-epl-navigation-recovery-diagnostic-20260715T134837Z.json`
  (`57de7968...`). Все три использовали zero paid traffic и зелёный cleanup.
- Добавлены настоящий process-global start governor (не менее 546 ms между
  actual browser `fetch`), общий persistent circuit 15/30/60 минут с одной
  half-open пробой и fail-closed secure state. Expected access gate и обычные
  502/timeout не открывают paid route. Isolated production suite: 1,429 tests;
  Ruff, compile, Compose render и diff check зелёные. Независимый review:
  `READY`, Critical/Important отсутствуют. Новый extension SHA-256 —
  `4e49832333664af3b773888bb1fbeb2fde7b9f41662029c817ced3176f09f249`.
  Этот exact extension развёрнут только в FlareSolverr container
  `e833962543d8e8526910e94425b34311b21cab006185dfeddf4f6b014740c733`;
  container healthy, restart count 0, OOM false.
- Первый live EPL replay нового build:
  `whoscored-workflow-epl-paced-circuit-replay-20260715T150426Z.json`, SHA-256
  `b4ecec7b2ce2087fb4e72f280facd314a43fa2261a42f7811e37019ec37ab45e`.
  Он за 12.452 s fail-closed остановился на authoritative FlareSolverr CF,
  использовал одну browser session, один direct FlareSolverr attempt и
  `paid_mb = 0.0`. Circuit открыл level-0 cooldown до
  `2026-07-15T15:20:07Z`.
- Bounded level-1 half-open replay:
  `whoscored-workflow-epl-level1-half-open-replay-20260715T155136Z.json`,
  SHA-256
  `e4c56d36484b6c1962610d3b79a064987554e66a2c2a95f697f03fa2496c3aee`.
  Он за 16.912 s сделал ровно один direct FlareSolverr attempt, снова получил
  authoritative CF, оставил `paid_mb = 0.0` и чистые browser sessions. Circuit
  перешёл на level 2 до `2026-07-15T16:52:14Z`.
- Bounded level-2 half-open replay:
  `whoscored-workflow-epl-level2-half-open-replay-20260715T165255Z.json`,
  SHA-256
  `ef2c171eb56cffd66d757e39096d1ce4d70358509621bacca52026a3663a2dcb`.
  Он за 7.052 s сделал ровно один direct FlareSolverr attempt, снова получил
  authoritative CF, оставил `paid_mb = 0.0`, clean browser sessions и healthy
  FlareSolverr с restart count 0/OOM false. Circuit generation 7 перешёл на
  level 3 с capped 60-minute cooldown до `2026-07-15T17:53:52Z`. Это внешний
  source/IP blocker: 6h canary и production promotion не запускаются, пока
  bounded EPL replay не станет зелёным.
- Подготовлен, но не активирован точечный Airflow runtime override
  `/root/fbref-949-runtime/whoscored-runtime-20260715.override.yaml`, SHA-256
  `cbcfe7f8a15959ab1bf8c51f5d20e36fa43a826b205f9994b6de8f853f33f4e4`.
  Compose render сохраняет pool 2 и fail-fast circuit; ephemeral Airflow
  runtime contract зелёный: parser v8, schema 3, 25 datasets, 9 files,
  code-tree SHA-256
  `a32bd6923600dd246487c0b103834cb2759ee9362bd1c12e32aa46b04906c39f`.

После green canary ещё обязательны продвижение pool/env до четырёх слотов,
один manual daily, один scheduled daily и единственный immutable `all_catalog`
plan с завершением и historical DQ не позднее 30 дней.

Операционные команды и критерии DQ приведены в
[`whoscored-production.md`](whoscored-production.md).

## Сессия 2026-07-18 — offline-верификация, изоляция шлюза, CF-egress проб

Работа велась в отдельном worktree `/root/dpf-ws954-work` (ветка
`ops/ws954-production` от `origin/master` `dc52465`), общий стек НЕ пересоздавался.
Evidence: `/root/whoscored-954-runtime/` (`verify-*.log`, `cf-egress-probe*.json`,
`PROBE-CONCLUSION-20260718.md`).

### Зелёное офлайн (без живого эгресса)
- Юнит-сьют whoscored+смежные: **2351 passed, 3 skipped, 1 flaky** —
  `test_whoscored_capacity_container_runtime::test_normal_exit_during_stats_is_reconciled_with_fresh_exact_inspect`
  падает ТОЛЬКО в общем прогоне, изолированно и файлом — зелёный (межтестовое
  загрязнение состояния на master, не продакшен-баг).
- `py_compile` всех whoscored dags/scripts: OK. `git diff --check`: чисто.
- Структурная валидация `docker compose config --no-interpolate`: shared compose OK
  (2745 стр.), новый gateway-файл OK (298 стр.). Полный интерполированный рендер
  shared-compose требует рантайм-секрет-путей (`:?`), что и есть fail-closed by design.
- Runtime preflight: parser `whoscored-parser-v8`, report schema 3, 25 datasets,
  classifier `senior-men-v3`; `test_whoscored_runtime_contract` — 44 passed.
- Silver charter `--check`: whoscored **чист** (3 находки — transfermarkt v2, чужие).
- DAG-import: `airflow dags list-import-errors` вернул **0** (снято с работающего
  scheduler'а в 05:34, когда он был функционален).

### Изоляция шлюза (закрыт SHARED-STACK-PROTOCOL gap)
- Создан `deploy/whoscored/gateway.compose.yaml` по образцу sofascore: платный трио
  (`whoscored_paid_gateway` + `flaresolverr_whoscored_paid` + `whoscored_proxy_filter`)
  + сети `dp-whoscored-paid-*` в собственном проекте `-p whoscored-gw`. Коммит на
  ветке (НЕ запушен). **Живой cutover НЕ делался** — процедура (флип сетей в external,
  удаление трио из общего compose, репойнт scheduler) описана в шапке файла, ждёт
  координированного окна. Запуск изолированно:
  `docker compose -p whoscored-gw -f deploy/whoscored/gateway.compose.yaml --project-directory . --env-file /root/data-platform-football/.env up -d`.

### CF-egress проб (ключевой результат, ~1 MB прокси-трафика)
Диагностик curl_cffi (browser-TLS, без JS), ≤512 KiB/запрос. НЕ production paid-run.
- **direct/host-IP реально Cloudflare-challenge'ится на EPL-странице**
  (`/Regions/252/Tournaments/2/…` → 403 `cf-mitigated: challenge`); homepage и
  `/Regions/` отдаются 200 — воспроизводит блокер.
- **резидентный прокси (order 38950) ту же EPL-страницу ПРОХОДИТ** → 200, 211 KB
  реального контента, без challenge. Пул живой, CF-репутация хорошая.
- feed `getteamstatistics`: 403 (нужен browser-derived `cf_clearance`); через прокси —
  challenge, НЕ hard-block → браузер (FlareSolverr) на этом IP фиды дотянет.
- Вывод: **прокси = рабочий чистый эгресс**; проблема — репутация именно host-IP
  (и хост, и прокси через FRA-эдж, но challenge'ится только хост).

### Рекомендация к продакшену
Самый дешёвый путь, совместимый с текущим кодом (`WHOSCORED_FULL_PAID_CRAWL_AVAILABLE=False`,
1 GiB прокси-бюджета, canary direct-only): **дать хосту чистый DIRECT-эгресс** (новый IP /
clean VPN-exit с CF-репутацией как у прокси). Тогда direct-only production (6h canary → daily →
backfill) работает как задумано, без per-byte затрат. Альтернатива (прокси для production) —
нереальна на 1 GiB + требует правки safety-кода.
Полное подтверждение data-пути = санкционированная `dag_canary_whoscored_proxy`
(browser+proxy) — gated свежим receipt (владелец даст) + `ready-v1` attestation
(сейчас `blocked-v1`).

### ⚠️ Замечание по общему стеку (не мой контейнер)
Общий `airflow-scheduler` (образ `ws954/*`, владелец — прошлая/другая whoscored-сессия)
на старте был UNHEALTHY (Up 8h) и в моё окно перемежающе крэш-лупил на entrypoint
`airflow db check` (exit 0, НЕ OOM; restart 0→7, затем стабилизировался). postgres
(66/100 conn), webserver, изолированные sofascore/fotmob scheduler'ы, trino — **все
healthy, платформа не задета**. Я поднял 0 контейнеров и общий стек не пересоздавал.
Оговорка: dag-import я снимал `airflow dags list-import-errors` ВНУТРИ этого общего
scheduler'а (read-only, успешно) — впредь такую валидацию делать в изолированном
throwaway, а не в хрупком общем scheduler.
