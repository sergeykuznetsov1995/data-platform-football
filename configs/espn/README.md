# ESPN configuration

## Denominator registry (`denominator.tsv`, #1499)

`denominator.tsv` is the one file that says which ESPN soccer leagues count as
ours. ESPN has no age or level field, so every league we know of gets a class,
and only adult men's official leagues form the target. The file also keeps the
leagues we do not collect (women, youth, friendlies…), so that daily discovery
of new leagues does not report them again. Loader:
`scrapers/espn/denominator.py` (`load_denominator()`; env
`ESPN_DENOMINATOR_PATH` overrides the path). Any format error fails closed.

Tab-separated, UTF-8, sorted by `slug`. Columns:

| Column | Meaning |
|---|---|
| `slug` | ESPN league slug, the key (equals bronze `competition_slug`) |
| `espn_id` | ESPN numeric league id (empty if not recorded) |
| `name` | ESPN league name |
| `espn_gender` | ESPN's raw `gender` (`MALE`, `FEMALE`, `UNKNOWN`; `FEMALE?` — guessed from the slug, detail never fetched) |
| `class` | one of the classes below |
| `class_reason` | `rule:<rule>` from the class rule or `manual:<text>` from a manual table |
| `in_target` | `1` exactly when `class = senior_official` and `hidden = 0`; the loader refuses anything else |
| `hidden` | `1` for a league ESPN serves by slug but does not list (`sui.1`): senior, outside the percentage, checked by hand weekly |
| `live` | `1` when the slug is in the core list `leagues?limit=500` of 24.09.2026 |
| `current_season_year` | ESPN `source_season_year` snapshot of 31.07/24.09; the live value is taken by #1501 |
| `deep_level` | empty until #1513 |
| `fotmob_id`, `sofascore_id` | empty; filled by hand when cross-source ids are unfrozen |
| `note` | free text from the 24.09 review |

### Classes and history-queue priority

| `class` | Priority | Meaning |
|---|---|---|
| `senior_official` | 1 | adult men's official competition — the target |
| `youth`, `olympic`, `friendly`, `reserve` | 9 | outside the percentage, history last |
| `women`, `college` | 0 | not collected |

The denominator of the freshness percentage is counted in matches by its
consumers (#1505); a target league without matches contributes nothing.

### Class rule

`scrapers/espn/classify.py`, in this order:

1. `MEN_OVERRIDE` (manual) → `senior_official`: `concacaf.champions_cup`, which
   ESPN marks `gender=FEMALE` by mistake;
2. `women`: `gender=FEMALE` (`rule:women_gender`), `WOMEN_RE` on the slug
   (`rule:women_slug`), or the word `women` in the name (`rule:women_name`);
3. `college`: slug `usa.ncaa*`;
4. `olympic`: `olympic` in the slug or name;
5. `youth`: `YOUTH_RE` (U15–U23) on the slug or name;
6. `friendly`: `fifa.friendly`, `club.friendly`, `nonfifa`, `friendly.*`, or
   `friendly` in the name (`rule:friendly`); then the manual `INVITATIONAL`
   table of preseason/invitational events (`esp.joan_gamper`,
   `jpn.world_challenge`, `fifa.intercontinental.cup`, `bangabandhu.cup`);
7. `reserve`: `reserve`, `II`, `.b`/`_b` suffix;
8. otherwise `senior_official` (`rule:default`). `eng.trophy`, `usa.open` and
   `bra.camp.*` stay in the target.

`tests/unit/scrapers/test_espn_classify.py` runs the rule over the recorded
catalog and fails if the file and the rule disagree.

### Adding or changing a league

Add or edit one row: `class` and `class_reason` from `classify()` (or a
`manual:` reason plus an entry in the manual table of `classify.py`), and
`in_target`/`hidden`/`live` consistent with it. New leagues come from
`scrapers/espn/catalog_core.py`: `diff_catalog()` against the core list, then
`propose_row()` over the league detail gives the candidate row.

Check:

```bash
pytest tests/unit/configs/test_espn_denominator_file.py \
  tests/unit/scrapers/test_espn_classify.py tests/unit/scrapers/test_espn_catalog_core.py -q
```

## Transport policy (`transport_policy.json`, #1500)

One gate per VM (`scrapers/espn/gate.py`, `TransportGate`) decides where and
when every ESPN request goes. Its state is one JSON file under `flock`
(`ESPN_GATE_STATE_PATH`, default `${AIRFLOW_HOME:-/opt/airflow}/state/espn/gate.json`),
so all processes share pace, origin blocks and counters. Loader:
`load_transport_policy()`; an unknown key, non-increasing steps, a share
outside (0, 1) or missing lanes fail closed.

| Key | Meaning |
|---|---|
| `clusters` | `site`: primary `site.web.api.espn.com`, reserve `site.api.espn.com` (closed by Akamai for our User-Agent since 04.08); `core`: `sports.core.api.espn.com`, no reserve |
| `origin_block_seconds`, `origin_probe_seconds` | a 403 closes an origin for 30 min; then exactly one request probes it every 30 min |
| `reserve_probe_seconds` | the reserve starts closed and is probed at most once a day |
| `all_blocked_pause_seconds`, `all_blocked_probe_seconds` | no open origin in a cluster: 30 min pause, then one probe every 5 min; `history` freezes first and reopens last |
| `steps` | pace ladder S0…S3, requests per minute; the ceiling is `ESPN_GATE_STEP_CEILING` (default 0 = S0; raising it is #1510) |
| `live_share` | share of each minute reserved for `live`: `history` is admitted only while its permits of the last minute stay below `1 - live_share` of the step |
| `lanes` | per-lane daily request fuse (`daily_requests`), set above the S3 maximum; UTC date. Bytes are counted per lane (state `daily`) but never capped: no daily MB ceilings (roadmap assumption 4) |
| `reset` | auto-reset: a 429, ≥ 3 × 403 in 60 s, or 5xx+timeouts > 2 % of ≥ 50 requests in 5 min → one step down, 15 min cooldown with `history` frozen, then back to the ceiling; two resets within an hour → S0 for 6 h and an `alert` in the state file |
| `uncompressed_warn_bytes` | an `identity` response larger than this is logged as a warning |

A 403 is never retried on another origin inside the same request: the request
fails with `OriginBlocked` (`AllOriginsBlocked` when the cluster has no open
origin), is journaled as `blocked_deferred`, and the caller retries it in a
later wave.

### Deep core: first stage (#1513)

The existing `dag_espn_history.run_history` first gives deep at most two minutes
of the shared twelve-minute run budget, then runs history until the original
deadline. It uses the same `espn_history` pool, with no additional workers or DAG.
The gate keeps `lane=history`, records `purpose=deep` in its locked permit state,
and admits at most 20% of the history minute quota (10% of the total rate).
Probes and retries consume that quota and the existing history daily fuse.
Every admission and wait checks live debt, protection, deadline and `history.off`;
missing live-debt evidence closes deep. No standalone live probe is provided.

`iceberg.ops.espn_depth_v1` is the season registry, keyed by
`(competition_slug, source_season_year)`; it does not change the league-wide
`denominator.tsv.deep_level`. Targets are the current seasons of all denominator
targets, independent of `history_scope.json`. A published played owner supplies
the sample for `plays?limit=1`. Counts >=1000 mean full, 100–400 medium, <50 light;
the gaps and failed probes remain unclassified. Seasons without a sample wait.
A new season gets a new registry key and probe. The map stores count, sample id,
checked time and raw response URI/SHA manifest. Only full editions collect data.

Stage one publishes `iceberg.bronze.espn_plays`, partitioned by competition and
integer source season, key `(event_id, play_id)`, plus team `deep_stats_json`.
The dedicated parser is `espn-deep-v1`. Every actual plays page is checked for
stable metadata, page order, total count and unique ids; no `limit=2000` assumption.
Team envelopes keep the manifest of every response and `players_state=pending`;
player statistics are a later stage. Raw bodies are flushed before publication.
All Summary and deep publications serialize per edition on the gate state volume
(`edition-locks/`), re-read Bronze under the lock and preserve first publication.
Plays and team rows precede the `deep_state=completed` match marker. A failed
stage remains resumable; Summary refreshes preserve existing deep JSON and plays.

Support for xG comes from positive source statistics/shots, never count or a
zero placeholder. Absent xG fields in complete content mean false; all-zero
fields remain unknown. Unless support is confirmed true, xG/xA/xGOT values are
NULL, including named statistics in JSON. A confirmed season retains real zeros;
previously completed matches with unknown support are upgraded on a later run.

The DAG result and log include requests, direct bytes, retries, cache hits,
completed/failed matches and requests/KB per completed match. Attempts and bytes
come from the durable attempt journal, including interrupted requests; when an
interruption leaves the request ledger incomplete, retries are unknown (`NULL`)
and `retry_attempts_lower_bound` is reported separately. Probe and failed-work
costs are included, so these are run costs amortized over completed matches.
Production depth coverage, API field shapes and actual cost still require source
acceptance after the active measurement/windows; offline tests use synthetic
core responses and recorded Summary fixtures. Delivery remains the existing
cron process and its current/history active-run guard.

## Tier A history acceptance (#1511)

`history_scope.json` pins the 2016–2025 window: ten past seasons for the five
major leagues and Champions/Europa League, Conference League 2021–2025, World
Cup 2018/2022 and Euro 2016/2020/2024 (2020 is the edition played in 2021).
The denominator is **80 pairs**, not 100 hypothetical editions. `allow` stays
nonempty: an empty list opens the whole target registry. `excluded` may contain
`[slug, year, reason]` only for an explicitly agreed, confirmed unavailable
edition already in `allow`; exclusions reduce both collection and denominator.
The initial exclusions list is empty. Changing it requires recording the
confirmation and owner's decision in the reason.

The checked-in registry is a snapshot: it still says current=2025 for the
three European club cups and current=2024 for Euro. Before delivery, verify
current season years through the established registry procedure. No source
requests were made for this implementation. The history lane retains its
`year < current_season_year` guard: with this snapshot it will mark the three
2025 cup pairs and Euro 2024 red, rather than collect them. Such exhausted
queue rows need the established retry procedure after the registry is corrected;
this change does not reset retries or silently advance registry years.

`scrapers.espn.history_acceptance` provides read-only report builders, separate
from the existing morning report and live-debt API:

```python
from scrapers.espn.history import load_scope
from scrapers.espn.history_acceptance import (
    core_evidence, render_acceptance_sql, render_acceptance,
    render_parity_sql, render_parity, render_legacy_parts_sql,
)

scope = load_scope()  # active pinned pairs, after explicit exclusions
# Saved season JSON + all saved event-list pages of every type; no HTTP here.
# pages_by_type = {type_id: [page_1_bytes, ..., page_N_bytes]}
# evidence[(slug, year)] = core_evidence(season_json, pages_by_type)
# quality_results[(slug, year)] = True / False / None  # pass / defect / unknown
# acceptance_sql = render_acceptance_sql(scope, evidence, quality_results)
# report = render_acceptance(rows_returned_by_read_only_query)
parity_sql = render_parity_sql(scope)
parts_sql = render_legacy_parts_sql(scope)
```

Supply evidence for every pair, sourced from the same saved core season/list
bodies used by the history lane (raw cache/export). `core_evidence` validates
all declared types, pages, original counts and within-type unique event IDs.
Do not reconstruct evidence from `queue.matches`: it has already lost cross-type
duplicates. The report shows sum of core counts, union of IDs, cross-type
repetitions, physical Bronze duplicate rows and missing/extra published Bronze IDs.
Physical duplicates make a pair red, including unpublished duplicate rows. Confirmed terminal nonplayed
Bronze rows also count as completed IDs even without `first_published_at`.
Missing evidence blocks non-red pairs. The required `quality_results` mapping
records each pair as True (checked, no defects), False (confirmed defects), or
None (unknown). Missing/unknown assessments block acceptance, including for red
pairs. Include capability-aware defects from `history_report` (valid-empty parts
are defects only where the season actually has that capability). Persist the
scope, raw evidence references, confirmed quality findings and report together;
an omitted quality assessment is not proof that quality was checked.

The acceptance SELECT LEFT JOINs from scope, not queue. Any red type makes its pair red once; failed inventory makes only the
not-yet-inventoried (absent) pairs red; an inventory failure does not add
a separate denominator row. Absent/pending/listed pairs and incomplete type
evidence block completion. Terminal red pairs count against the strict
`red * 5 < N` threshold; retryable red rows (`attempts < MAX_ATTEMPTS`)
still block completion; failed discovery may explain absent evidence for a red
pair. A completed pair with missing/extra published IDs is red even if queue
counts agree. The renderer includes every pair and its discrepancy fields.

Parity covers the six legacy leagues (top five + World Cup; the legacy DAG
uses `INT-World Cup`, not Champions League). World Cup uses the four-digit
calendar year as its legacy season label. It joins
native `event_id` to schedule `game_id` within competition, across legacy season
labels; dates and scores are checked separately. Output contains native-only,
legacy-only, duplicate/ambiguous links, NULL comparisons and wrong season labels.
Native parity includes confirmed terminal nonplayed rows without publication.
Unmatched legacy rows are selected by label or date: July-to-July for leagues,
calendar year for World Cups. The appended `date_seasons` column lists date-based
candidate years; a conflict with the label marks an unmatched row ambiguous.
These calendar hints do not prove season membership for extended seasons.
`ITA-Serie A/2021` is explicitly excluded as the known copy of 2019/20 and
counted in the parts report. Lineup/matchsheet coverage follows schedule links
by native event ID, schedule dates or season labels, retaining the original
legacy label. Rows without exactly one schedule
link are uncomparable. Their scores are not a parity reference; score comparison
uses schedule's `home_goals/away_goals` and leaves NULL as unknown. Legacy is
never the completeness baseline. SQL is SELECT-only over Bronze/ops, no Silver
or Gold transforms and no Bronze schema/parser/batch-identity changes.

Delivery and actual issue acceptance remain operational steps after #1510's
measurement window under the source protocol. Do not deliver during the window,
change gate/workers, reset retries, or bypass the automatic delivery lock. The
existing history lane's live-debt pause, budgets, stop file and quality gates
continue to apply; no new DAG or manual queue population is needed.
