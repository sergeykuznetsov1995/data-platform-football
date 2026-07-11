# FotMob: воспроизводимый benchmark baseline

Этот документ фиксирует методику замера. Скрипты работают без Trino, S3 и
Airflow writes: единственный side effect — локальные артефакты в указанном
`/tmp`-каталоге.

## Зафиксированный legacy baseline

Операционная реконструкция до рефакторинга для `EPL + World Cup`:

| Метрика | Значение |
|---|---:|
| Wall time одного запуска | 8:37 (517 с) |
| HTTP GET | 305 |
| Proxy traffic | 0.00 MB |
| Частота | 2 запуска/сутки |
| Суточная стоимость | ~610 GET, ~17:12 wall time |

Этот legacy scope уже, чем новый fixed-sentinel scope, поэтому цифры нельзя
сравнивать напрямую. Сопоставимый before/after строится следующими командами.

## Fixed-sentinel scope

| Ключ | Competition ID | Exact source season key |
|---|---:|---|
| EPL | 47 | `2025/2026` |
| Champions League | 42 | `2025/2026` |
| Champions League Qualification | 10611 | `2025/2026` |
| Nations League A | 9806 | `2024/2025` |
| Russian Premier League | 63 | `2025/2026` |
| Africa Cup of Nations | 289 | `2025` |

Ключ сезона передаётся FotMob без вычисления из года. Ответ считается полным
только при точном совпадении `details.selectedSeason`; отсутствие ключа можно
понизить до warning лишь явным флагом `--allow-missing-selected-season`.

## Команды

Cold baseline независимым transport и offline replay старого summary:

```bash
python scripts/research/bench_fotmob_fetch.py \
  --transport standalone \
  --label baseline-fixed \
  --artifact-dir /tmp/fotmob-benchmark/baseline-fixed

python scripts/research/bench_fotmob_replay.py \
  --artifact-dir /tmp/fotmob-benchmark/baseline-fixed \
  --parser standalone \
  --iterations 5
```

Cold и warm замеры refactored transport используют один raw-store. Отдельные
report paths не дают второму запуску затереть первый:

```bash
python scripts/research/bench_fotmob_fetch.py \
  --transport canonical \
  --label after-cold \
  --artifact-dir /tmp/fotmob-benchmark/after \
  --report /tmp/fotmob-benchmark/after/fetch-cold.json

python scripts/research/bench_fotmob_fetch.py \
  --transport canonical \
  --label after-warm \
  --artifact-dir /tmp/fotmob-benchmark/after \
  --report /tmp/fotmob-benchmark/after/fetch-warm.json

python scripts/research/bench_fotmob_replay.py \
  --fetch-report /tmp/fotmob-benchmark/after/fetch-cold.json \
  --parser auto \
  --iterations 5 \
  --output /tmp/fotmob-benchmark/after/replay.json
```

`--parser auto` лениво подключает
`scrapers.fotmob.parsers:parse_season_bundle`; на baseline checkout без этого
модуля используется standalone summary.

## Интерпретация метрик

- `attempts`, `retries` и `status_counts` включают все HTTP attempts, а не
  только успешные logical targets.
- `encoded_*_bytes` — размер HTTP body на проводе после transfer framing, но до
  `Content-Encoding` decode. Это основная traffic-метрика.
- `decoded_*_bytes` — размер JSON payload, который обработал parser. В warm
  canonical run он может прийти из локального cache после HTTP 304.
- Headers, TLS и connection overhead не измеряются; это ограничение записано в
  каждый fetch report.
- Proxy разрешён только в standalone режиме и только явным `--proxy-url`;
  environment proxies игнорируются. Canonical transport всегда direct-only.
- Каждый payload сохраняется как content-addressed gzip и проверяется двумя
  SHA-256: exact decoded body и canonical JSON. Replay отказывается работать с
  отсутствующим, повреждённым или помеченным failed target.
- Replay выполняет JSON decode и parsing на каждой из пяти итераций, проверяет
  детерминизм результата и публикует p50/p95, row counts, collection hashes и
  union field inventory.

Для корректного before/after live fetch следует запускать подряд: FotMob может
изменить fixtures между замерами. Parser speed и полноту сравнивают на одном и
том же сохранённом raw corpus, где source drift исключён.

## Итог реализации и измерения — 2026-07-11

### Сопоставимый before/after на одном fixed corpus

Все шесть cold payload hashes совпали до байта между standalone baseline и
canonical transport. Поэтому различия row counts ниже — это исправления
парсинга, а не изменение источника.

| Метрика | Baseline standalone | Native cold | Native warm |
|---|---:|---:|---:|
| Logical targets / HTTP attempts | 6 / 6 | 6 / 6 | 6 / 6 |
| Wall time | 10.746 s | 10.483 s | 10.152 s |
| Encoded direct body | 0.259185 MiB | 0.259185 MiB | **0 MiB** |
| Proxy body | 0 MiB | 0 MiB | 0 MiB |
| Retries / failed targets | 0 / 0 | 0 / 0 | 0 / 0 |
| Raw hits / writes | 0 / 6 | 0 / 6 | 6 / 0 |

Warm run получил шесть `304 Not Modified`: request headers/TLS не входят в
body-byte метрику, но повторная передача JSON body устранена полностью. Cold
transport не должен обещать уменьшение source payload — его доказательство
корректности состоит в одинаковых hashes и точном учёте bytes.

На тех же 3.176439 MiB decoded raw, 5 итераций без сети:

| Parser replay | p50 total | p50 / payload | p95 total |
|---|---:|---:|---:|
| Baseline source walker | 0.709932 s | 0.118322 s | 0.798815 s |
| Native one-pass parser | **0.328540 s** | **0.054757 s** | **0.408823 s** |

Native p50 быстрее на 53.7%. Он вернул 1,043 уникальных матча, 538
context-grained standings rows, 181 stage, 47 playoff matchup, 166 команд и
402 leaderboard category без parse issues. Старый диагностический walker видел
те же 1,043 unique match ID, но насчитал 2,086 occurrences и только 236
standings rows: он одновременно дублировал матчи и пропускал table contexts.

### Доказательство динамического каталога и сезонов

Live `allLeagues` audit:

- 565 source occurrences сведены в **555 уникальных numeric competition ID**;
- 493 турнира классифицированы как official adult-men, 62 явно исключены;
- 0 конфликтов numeric ID, 0 invalid entries, 0 unknown JSON paths;
- один первоначально неизвестный root `popular` был сохранён raw и заблокировал
  публикацию до добавления явного `raw_only` правила;
- proxy bytes = 0; warm catalog response = 304 / 0 body bytes.

Все 16 acceptance identities присутствуют и прошли exact-season parsing:

| Scope | IDs | Season counts | Selected samples |
|---|---|---:|---|
| EPL / RPL | 47, 63 | 17 / 17 | `2026/2027` |
| UCL / qualification | 42, 10611 | 16 / 17 | `2025/2026`, `2026/2027` |
| Nations A-D | 9806-9809 | 5 each | `2026/2027` |
| Nations qualifications | 10557, 10558, 10717-10719 | 2, 2, 1, 1, 1 | `2025`, `2024/2025` |
| RPL qualification | 9333 | 11 | `2025/2026` |
| AFCON / qualification | 289, 10608 | 9 / 10 | `2025`, `2026/2027` |

Mandatory current payloads produced 0 parse issues and 0 unknown fields.
Notably, AFCON qualification preserves both `2017/2019` and `2017/2018` as
different keys. Display-only champion labels such as `2025 Morocco` live in a
separate season-history entity and are never sent back as API season keys.
Qualifiers with no standings still retain 4-56 teams through fixtureInfo and
home/away/playoff unions.

### Реализованная source-native карта сущностей

| Canonical table | Grain / source content |
|---|---|
| `fotmob_competitions` | complete catalog snapshot + scope decision/tombstone |
| `fotmob_competition_seasons` | numeric competition + exact source season key |
| `fotmob_competition_season_history` | display history, champion/runner-up |
| `fotmob_season_stages` | fixture, table, stats and playoff stages |
| `fotmob_matches` | deduplicated fixture/match identity and status |
| `fotmob_standings` | stage/table/group/type/team context; duplicates are explicit |
| `fotmob_playoff_brackets` | rounds, ties, aggregate result and match IDs |
| `fotmob_season_teams` | union of fixtureInfo, matches, tables and playoffs |
| `fotmob_leaderboard_categories` | every advertised player/team category, URL and source order, including unavailable categories |
| `fotmob_leaderboards` | every advertised URL and every `TopLists` block |
| `fotmob_match_payloads` | match facts/stats/player stats/lineup/shotmap/momentum masks |
| `fotmob_team_snapshots` | global observed-at team profile, never fake historical |
| `fotmob_squad_snapshots` | global observed-at players/coaches |
| `fotmob_player_snapshots` | global Next profile/career/value/trophy snapshot |
| `fotmob_transfer_events` | league-filtered, paginated global event identity |
| `fotmob_field_inventory` | every observed path and typed/raw/excluded disposition |
| `fotmob_ingest_manifest` | logical commit, raw URI/hash, counts, capabilities and traffic |

Raw JSON is content-addressed gzip with SHA validation. Physical entity rows are
append-only and become visible through `*_current` views only after a successful
manifest commit. Unknown paths stay raw and end in `schema_drift`; 404/204,
retryable and terminal failures are distinct states. Catalog deletion requires
absence from two consecutive complete snapshots.

### Намеренные исключения

Scope policy stores, but does not ingest, women/female, U17-U23/youth,
reserve/development/PL2, friendly, charity, exhibition and testimonial
competitions. Unknown explicit gender/age metadata becomes `review_required`;
hooks can resolve it audibly. This is an ingest exclusion, not catalog loss.

Field exclusions are deliberately tiny and remain in raw JSON:

- `content.buzz.*`: ephemeral engagement/UI widget;
- team `fixtures.*`: duplicate of canonical competition match ingestion.

UI navigation/localization, empty `QAData`, overview widgets, history fragments
and similar known fields are `raw_only`, not discarded. Every disposition and
reason is exported by `field_map.py` and written to the field inventory.

### Инкрементальность, traffic и эксплуатация

- exact URL+sorted params cache; ETag/Last-Modified; validated 304 replay;
- byte-identical successful targets reuse their committed physical batch;
  cache presence alone can never publish rows after an interrupted commit;
- snapshot-backed current views first select the newest successful exact target,
  so removed/postponed fixtures and departed squad members do not survive from
  an older payload; catalog tombstones retain their separate two-snapshot rule;
- immutable finished matches skip successful manifests before applying batch
  limits, so each daily chunk progresses instead of refetching its prefix;
- team snapshots have a 20-hour freshness window, player snapshots seven days;
- one league payload feeds season/matches/tables/stages/teams/categories and the
  selected discovery response is reused (no second season GET);
- fixture `pageUrl` removes the old extra `/match?id=` call;
- transfers use `leagueIds`, walk pages until unique events reach `hits`, and
  never replicate a global event into historical season partitions; all-time
  and one-year page identities are separate, resumed pages replay raw, and a
  conditionally checked page-one hash invalidates stale checkpoints on shifts;
- daily scopes and transfer streams are ordered by their oldest completion
  timestamp, preventing a frequently refreshed prefix from starving the rest
  of the dynamically discovered catalog;
- defaults: 30 requests/min, 4 workers, 2,000 requests, 256 MiB direct, 0 proxy;
- backfill priority: mandatory identities, selected/latest seasons, then older
  source order; a scope is skipped only after a plan-signature completion
  marker proves every requested child entity, not merely the season payload;
- work is fair and scope-by-scope (season → leaderboards → matches → teams →
  squad-derived players); a budget-limited retry replays the successful exact
  season raw with zero HTTP and continues the remaining child identities;
- fresh team snapshots still recover player IDs from the latest committed
  squad, and player limits apply after freshness filtering, so a cached prefix
  cannot starve later players;
- budget/limit deferrals and unexplained requested 404s are explicit
  incomplete/retryable outcomes; only a policy-recorded unavailable category
  is terminally acceptable;
- CLI modes: `discover`, `daily`, `backfill`, `replay`; reports are atomic and
  run-specific; incomplete/schema-drift runs exit non-zero.

### Legacy migration and remaining risks

Legacy CLI remains additive for current consumers, with corrected traffic,
season validation, tables, leaderboards, transfer fields and match request
count. `migrate_fotmob_native.py` is audit-only by default; its explicit
two-step apply mode copies confirmed season-replicated team/player/transfer
snapshots into quarantine, validates counts, then deletes exact copied rows.

Silver/Gold intentionally remain limited to their existing mapped legacy
competitions during this rollout; the 555-ID canonical Bronze catalog is not
silently coerced into those names. A production migration should first finish
the staged canonical backfill and parity-check mapped IDs before switching
legacy consumers. The implementation does not claim that all 493 included
competitions were written to production Iceberg during this audit: live writes
were deliberately avoided; the full catalog and all mandatory roots were
validated read-only, while the bounded runner performs the resumable write.

FotMob's API is public but unversioned. New top-level fields intentionally stop
typed publication until classified. Transfers can change while paging; unique
event IDs plus `hits` validation turn that race into an explicit incomplete run
that retries later. Resume invalidates cached later pages when page one changes;
an edit confined to an older page without changing page one or `hits` remains a
source-side edge case and is handled by a future plan-version refresh/full
replay. Source scope heuristics should be periodically reviewed, especially for
competitions whose names do not carry gender/age metadata.

### Финальная верификация

- полный repository unit-suite: **3,769 passed, 34 skipped**;
- расширенный FotMob/runner/DAG/research/migration набор: **201 passed**;
- отдельные resumability/current-view/checkpoint проверки: **61 passed**;
- Ruff lint и format-check: успешно;
- `git diff --check`: успешно;
- `docker compose config --quiet`: успешно с ожидаемыми предупреждениями о
  секретах, отсутствующих в локальном audit shell.
