# SofaScore scale-out to all adult men's leagues (issue #946, phase 3)

Design document only — no implementation. It answers the four phase-3
questions from the owner plan: weekend capacity under current limits, budget
classes decoupled from one tournament, positive adult-men evidence, and an
auto-onboarding path, followed by a cost/time plan with explicit forks.
All code references are against worktree HEAD `b3e0f80`.

Measured byte figures below come from
`docs/research/sofascore-proxy-budget.md`: the 25-match and 50-player batch
figures were observed under the earlier runtime fingerprint `a3571299…`, the
season-shape figure under `960283608c…` (ibid., "Measured readiness
evidence"). Both fingerprints are already retired: the shipped artifact
`configs/sofascore/proxy_budget_canary.json` now carries digest `8d79b363…`
with empty `samples` and `hard_task_bytes: null` for every class, and the
phase-2 failover fix will rotate the fingerprint again. Every number in the
capacity model is therefore a planning estimate, to be re-derived from the
post-phase-2 canary re-collection; none of it can authorize production.

## 1. The constraint set as shipped

- Production paid traffic is closed: `SOFASCORE_DAGRUN_BUDGET_BYTES = 0` in
  `scripts/proxy_filter/filter_proxy.py` (`:114`); `main` replaces it only
  after loading a `verified: true` canary artifact, and the artifact is
  `verified: false` with zero samples.
- Six workload classes exist, all bound to `source_tournament_id` 16 (World
  Cup) or 17 (EPL): `match_batch_25_t{16,17}`, `player_batch_50_t{16,17}`,
  `season_16_b345be809034c692`, `season_17_4a1738f5b7504ec2`
  (`configs/sofascore/proxy_budget_canary.json`). The EPL names are also
  hardcoded as compatibility aliases `MATCH_WORKLOAD_CLASS` /
  `PLAYER_WORKLOAD_CLASS` in `scrapers/sofascore/workload_plan.py:52-53`.
- Verification requires, per class: ≥20 complete cold samples over ≥5
  distinct hashed exits (`requirements` in the artifact; enforced by the
  production loader `load_verified_workload_policy`,
  `workload_plan.py:530-533,605-608`) plus the three benchmark modes
  `no_op`, `offline_replay`, `single_endpoint_resume` — the modes are
  enforced by the collector's separate `verify` gate (`BENCHMARK_ONLY_MODES`,
  `scripts/research/bench_sofascore_paid_canary.py:135,1735-1743`), not by
  the production loader. `hard_task_bytes` must equal the maximum observed
  cold total (`workload_plan.py:609-613`); no multiplier is ever accepted
  (`workload_plan.py:462-463,477-480`).
- Shared limits: 100 MiB/day across **all** paid sources
  (`PROXY_FILTER_DAILY_BUDGET_MB=100`, `.env.example:151`;
  `DAILY_BUDGET_BYTES`, `filter_proxy.py:101`); 24 MiB max lease
  (`filter_proxy.py:98`), lease TTL 3600 s (`filter_proxy.py:100`,
  `SOFASCORE_PROXY_LEASE_TTL_SECONDS=3600` in `.env.example:141`); exactly
  one active SofaScore lease (`_create_lease`,
  `filter_proxy.py:1373-1376`); 20 requests/min
  (`scrapers/utils/rate_limiter.py:170`); the production DAG is serialized
  with `max_active_tasks=1` (`dags/dag_ingest_sofascore.py:1037`).
- Registry `configs/sofascore/tournaments.json`: 12 tournaments, only 16 and
  17 `enabled`, 10 rows have `classification.gender = "unknown"` — their only
  evidence is the schema-v1 migration stub
  (`"endpoint": "configs/sofascore/tournaments.json@schema-v1"`), i.e. no
  live discovery has ever refreshed them.
- Activation is fail-closed: `activation_eligibility`
  (`scrapers/sofascore/registry.py:229-288`) requires confirmed source
  `sport=football` and `gender=male`, an approved operator review confirming
  `{sport, gender=male, age_group=adult, team_level=first_team}` with
  evidence, and at least one canonical source season. Name heuristics only
  exclude, never confirm (`classify_tournament_source`,
  `registry.py:99-194`; `normalize_gender`, `registry.py:67-79` returns
  `unknown` for a missing field).
- Onboarding is one tournament per CLI call with a hand-typed
  `--canonical-id` (`dags/scripts/manage_sofascore_registry.py:33-51`), and
  the canonical link is `configs/medallion/competitions.yaml` (`id` ==
  registry `canonical_id`; sofascore appears there only inside
  `sources.primary`/`sources.fallback`). The DAG refuses any enabled league
  absent from `competitions.yaml` or without a matching canonical season
  (`_load_active_sofascore_leagues`, `dag_ingest_sofascore.py:162-227`).

## 2. Capacity math: leagues per weekend under current limits

### 2.1 Per-league weekly cost model (EPL-shaped league)

Measured class costs (retired fingerprint, one cold run):

| class | provider bytes | navigations | source |
| --- | ---: | ---: | --- |
| season shape (EPL 25/26) | 463,018 B (0.442 MiB) | 39 | sofascore-proxy-budget.md |
| 25-match batch (5 endpoints/match) | 1,256,896 B (1.199 MiB) | 126 | ibid.; old transport was 2,333,024 B |
| 50-player batch (2 endpoints/player) | 332,395 B (0.317 MiB) | 101 | ibid. |

Weekly increment for one EPL-shaped league (weekend cadence, i.e. one run
per week):

- **Season phase**: one bounded season capture — 0.442 MiB, 39 requests.
  The season freshness key rotates daily
  (`_season_freshness_key`, `dags/scripts/prepare_sofascore_workload.py:173-176`),
  so under today's *daily* master-pipeline trigger this is spent every day,
  not weekly — 7 × 0.442 ≈ 3.1 MiB/week/league. Weekly cadence requires
  gating the trigger or the season phase (phase-4 change).
- **Match phase**: ≈10 newly finished matches per round-week (380 matches /
  38 rounds; up to ~20 with a midweek round). Matches are captured once
  (match freshness is `"final"`, `prepare_sofascore_workload.py:211`;
  pending set = finished matches not yet terminal in the manifest,
  `_finished_match_ids` + `_pending_targets`,
  `prepare_sofascore_workload.py:123-130,166-172`).
  Actual bytes ≈ 10 × (1,256,896 / 25) = 0.479 MiB; **reserved** budget is
  the full measured batch ceiling 1.199 MiB, because smaller batches use the
  measured 25-batch cap, never a linear estimate (derivation contract in
  sofascore-proxy-budget.md; `class_for`, `workload_plan.py:360-390`).
  Requests: 5 × 10 + 1 warm-up = 51.
- **Players phase** (Saturday gate `_gate_player_capture`,
  `dag_ingest_sofascore.py:804-862`; weekly freshness key
  `week-%G-W%V`, `prepare_sofascore_workload.py:179-182`): the planned
  universe is the Bronze-observed union — 677 players for EPL 2025/26
  (`_observed_player_ids`, `prepare_sofascore_workload.py:133-151`; the
  496/677 coverage figure in sofascore-proxy-budget.md; the "~526" in the
  DAG docstring, `dag_ingest_sofascore.py:21`, is a legacy estimate) →
  14 batches of ≤50.
  Actual ≈ 677 × (332,395 / 50) = 4.292 MiB; reserved 14 × 0.317 =
  4.438 MiB. Requests: 2 × 677 + 14 warm-ups = 1,368.

Per-league weekly totals:

| estimate | MiB/league/week | basis |
| --- | ---: | --- |
| conservative | 6.08 | reserved allocation caps: 0.442 + 1.199 + 14×0.317 |
| middle | 5.21 | measured actuals: 0.442 + 0.479 + 4.292 |
| optimistic | 1.99 | actuals with the player phase rotated 1-in-4 weekends per league |

Time per league per weekly run: 39 + 51 + 1,368 = 1,458 requests. At the
20 req/min limiter this is ≥72.9 min; the measured live pace (p50 ≈ 3.0 s
per endpoint navigation) gives the same ≈73 min. Without the player phase:
90 requests ≈ 4.5 min. Everything is strictly serial (one SofaScore lease,
`max_active_tasks=1`), so the **time ceiling is ≈19 player-phase leagues per
day, ≈39 per 48 h weekend**. Lease TTL and the 24 MiB lease cap do not
bind: the largest single allocation is 1.199 MiB and the longest batch runs
≈6.3 min.

### 2.2 Shared daily budget scenarios

The 100 MiB/day budget is shared with Transfermarkt, FotMob, WhoScored and
SoFIFA. Leagues per weekend = ⌊2 × daily share ÷ per-league cost⌋:

| SofaScore daily share | conservative (6.08 MiB) | middle (5.21 MiB) | optimistic (1.99 MiB) |
| ---: | ---: | ---: | ---: |
| 30 MiB (TM+FotMob active) | 9 | 11 | 30 |
| 50 MiB | 16 | 19 | 50 |
| 70 MiB (SofaScore-dominant weekend) | 23 | 26 | 70 |

All cells are byte-bound except that weekly-players scenarios saturate the
time ceiling near 39; the optimistic column stays within time because only a
quarter of leagues run players on a given weekend (checked: 70 leagues ≈
25 h of serial capture). Caveats: smaller leagues have smaller player
universes than EPL's 677, so the middle column is slightly pessimistic for
them; cup-shaped competitions (WC/Euro) burst all their matches into a few
weeks instead of a steady 10/week.

**Honest bottom line**: with per-tournament classes as shipped, the answer
to "how many leagues fit" is **2** (t16, t17) — and **0** until the
post-phase-2 re-collection re-verifies them. The table above describes the
regime after sections 3–5 are resolved.

### 2.3 One-time onboarding cost per league (status-quo classes)

Verification of one new tournament under the current schema = 20 cold
samples of each of its three classes:

```
20 × (1,256,896 + 332,395 + 463,018) B = 20 × 2,052,309 B
                                       = 41,046,180 B ≈ 39.1 MiB
```

This matches the owner's ≈40 MiB estimate (20 × (1.2 + 0.33 + 0.44) MiB).
Of the benchmark modes, `no_op` and `offline_replay` are network-free;
`single_endpoint_resume` is a **paid live** mode
(collector docstring, `bench_sofascore_paid_canary.py:16-18`;
`live = mode in {"cold", "single_endpoint_resume"}`, `:726`) but costs one
resumed endpoint per class — noise next to the 39 MiB. On top of bytes,
every canary sample runs under a **globally isolated** lease — no other paid
source may run while it is active (`filter_proxy.py:1379-1382`) — costing
≈3.9 h of exclusive paid-proxy lock per league (20 × (5.4 + 4.2 + 2.0) min).

| leagues onboarded | paid MiB | calendar days @30 MiB/day | @50 | @70 | exclusive lock |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 5 | 196 | 7 | 4 | 3 | ~20 h |
| 10 | 391 | 14 | 8 | 6 | ~39 h |
| 20 | 783 | 27 | 16 | 12 | ~78 h |

**These are lower bounds** under wave onboarding: they price only the new
leagues' samples and assume the runtime fingerprint never rotates between
waves. Section 2.4 shows why that assumption fails for any onboarding that
touches `competitions.yaml` — and why variant C fails it on every single
tournament. This linear cost plus the cascade below is the main scale
blocker and the motivation for section 3.

### 2.4 The runtime-fingerprint invalidation cascade

The fingerprint input set (`RUNTIME_FILE_ENTRIES`,
`scrapers/sofascore/runtime_fingerprint.py:26-45`) covers not only the
capture/proxy code (`scrapers/sofascore/`, `scripts/proxy_filter/`, the DAG
and its scripts, `scrapers/utils/rate_limiter.py`, Dockerfile and dependency
pins) but also two entries with outsized scale-out consequences:

- **`configs/medallion/competitions.yaml`** (`runtime_fingerprint.py:40`).
  `validate_runtime_fingerprint` requires exact digest equality
  (`runtime_fingerprint.py:164-179`), and the loader requires every sample's
  `runtime_fingerprint_digest` to equal the artifact's
  (`workload_plan.py:541-548`); samples are never relabelled
  (sofascore-proxy-budget.md). Therefore **every merged auto-onboarding PR
  from §5.3, and every routine yearly season row** (e.g. adding `2627`),
  rotates the digest and instantly invalidates **all samples of all
  classes** — production falls back to
  `SOFASCORE_DAGRUN_BUDGET_BYTES = 0` until a full re-collection
  (≈48 MiB merged-shape / ≈78 MiB status quo, section 3.3) re-verifies.
  Even at a constant league count, season maintenance already implies at
  least one planned rotation + full re-collection per year.
- **`scripts/research/bench_sofascore_paid_canary.py`** itself
  (`runtime_fingerprint.py:36`). The collector hardcodes
  `REQUIRED_WORKLOAD_CLASSES` for t16/t17
  (`bench_sofascore_paid_canary.py:152-159`), so under variant C onboarding
  *any* tournament means editing the collector — which is a fingerprint
  input — i.e. a rotation even when `competitions.yaml` is untouched.
  Variant C is therefore not merely linear in cost: each onboarding also
  burns every previously verified class.

Consequences for the tables in §2.3 and the stage plan in §6: treat them as
lower bounds; the true cost of a wave that touches any fingerprint input is
`(new classes) + (full re-collection of every existing class)`. Mitigation
adopted in this plan (fork F2): batch **all** `competitions.yaml` edits of a
wave — new entries, season rows, sources changes — into one rotation,
immediately followed by one re-collection window. A longer-term option is to
narrow the fingerprint's `competitions.yaml` input to a SofaScore-relevant
projection, but that deliberately weakens provenance and needs its own
owner review; this document does not assume it.

## 3. Budget classes keyed by league shape

### 3.1 What binds a class to one tournament today

- Schema: `workloadClass.required` includes `source_tournament_id`
  (`configs/sofascore/proxy_budget_canary.schema.json`, `$defs.workloadClass`).
- Loader: class names must equal
  `match_batch_25_t{id}` / `player_batch_50_t{id}`
  (`workload_plan.py:495-508`); every sample's `source_tournament_id` must
  equal the class's (`:567-571`); season classes must end with their
  `shape_digest[:16]` and the digested shape must carry the same tournament
  (`:614-645`).
- Shape: `production_season_shape` embeds `source_tournament_id` **inside**
  the digested payload (`workload_plan.py:320-331`), so two identically
  shaped leagues can never share a `shape_digest` today.
- Planner: `WorkloadBudgetPolicy.class_for` re-checks tournament identity on
  every allocation (`workload_plan.py:378-383`), and
  `build_signed_dagrun_plan` derives class names from the tournament id
  (`:1117,1139`).

### 3.2 Options

**A. Shape-keyed classes (recommended).** A class is keyed by a
`shape_digest` computed from the league's *form*, not its identity:

- match scope: `{shape_version, scope, batch_size=25, required_endpoints
  [event, incidents, lineups, shotmap, statistics], transport_id}` —
  effectively one universal class for all leagues;
- player scope: analogous with `batch_size=50`,
  `[player_profile, player_season_statistics]`;
- season scope: today's shape minus `source_tournament_id`, plus form fields
  that actually drive bytes: `season_format`
  (`split_year`/`calendar_year`/`named`), the
  `schedule_page_chain.max_pages_per_direction=50` bound, static/dynamic
  endpoint families, and a **team-count band** (e.g. 16–20 vs 24–48) sourced
  from `competitions.yaml` `team_count` / `match_count`.

The class stores `measured_tournament_ids` (provenance) instead of one
`source_tournament_id`; each cold sample still records the tournament it was
measured on. Admission rule: a tournament may draw on a class iff its
*computed* shape digest equals the class digest and the runtime fingerprint
matches. New invariant: a class may authorize a tournament **outside**
`measured_tournament_ids` only if samples span ≥2 distinct tournaments —
this prevents generalizing from a single league's idiosyncrasies. All
existing invariants stay: per-request byte maps (`_validate_request_map`,
`workload_plan.py:393-425`), max-observed `hard_task_bytes`, ≥20 cold × ≥5
exits per class, no multipliers, fail-closed on any unmeasured shape
(`class_for` miss → `WorkloadPolicyUnavailable`).

**B. Per-tournament classes + transfer attestation.** Keep t-id classes; a
new league may borrow a donor class after an operator-approved attestation
plus k spot-check cold samples (e.g. 3) that must fit under the donor cap.
Cost ≈ 3 × 1.96 ≈ 5.9 MiB/league. Rejected as primary: the cap is then not
derived from the authorized league's own observed maximum, which quietly
breaks the `max_observed_task_bytes_per_workload_class_v2` contract; a
league whose 4th-through-20th samples would have exceeded the donor max is
admitted anyway. To be fair, variant A admits an even stronger form of the
same objection — a league whose shape digest matches can draw on the class
with **zero** samples of its own. The difference is what stands in for those
samples: under A, a machine-checked proof of *form equality* (same endpoint
families, batch sizes, bounded page chains — the byte-driving inputs) plus
the ≥2-tournament sampling invariant, so the cap already reflects
cross-league variance within the form and any residual outlier hits the
hard stop; under B, an operator waiver plus 3 samples bridges two forms that
were *never shown equal*, so the cap silently stops meaning "maximum
observed for this task shape". A keeps the derivation contract intact; B
redefines it.

**C. Status quo.** ≈39 MiB and ≈4 h of exclusive canary lock per league
(section 2.3). Safe, honest, and unaffordable at 20+ leagues.

### 3.3 Migration path (variant A)

1. Bump artifact schema to v3: `workloadClass` drops required
   `source_tournament_id`, gains required `shape`, `shape_digest`,
   `measured_tournament_ids`; `coldSample.source_tournament_id` is kept as
   provenance and validated for shape membership instead of identity.
2. `workload_plan.py`: re-key `match_workload_class` /
   `player_workload_class` / `season_workload_class` to shape tokens; move
   `source_tournament_id` out of the digested season shape (sibling
   provenance field); replace identity checks in `class_for` and
   `load_verified_workload_policy` with digest-equality plus the
   ≥2-tournament generalization invariant.
3. Follow through `scripts/proxy_filter/budget.py` and `filter_proxy.py`
   (both consume the policy via `load_verified_workload_policy`) and the
   canary collector `scripts/research/bench_sofascore_paid_canary.py`.
4. **Class declarations must move out of fingerprint-hashed code.** Today
   the required-class list is hardcoded in the collector
   (`REQUIRED_WORKLOAD_CLASSES`, `bench_sofascore_paid_canary.py:152-159`),
   which is itself a fingerprint input (`runtime_fingerprint.py:36`) — so
   declaring a new class would rotate the fingerprint and burn every
   existing class (§2.4). In v3 the collector must derive its class set from
   non-fingerprint inputs: the artifact's own
   `requirements.required_workload_classes` plus the cohort configs
   (`configs/sofascore/proxy_canary_cohort*.json`) — none of which are in
   `RUNTIME_FILE_ENTRIES`. This is a **precondition** for the
   "≈8.8 MiB per new shape" marginal cost below being real; without it,
   every new shape costs a full re-collection.
5. **Sequencing is the whole point**: `workload_plan.py`, `budget.py` and
   `filter_proxy.py` are all in the runtime-fingerprint file list
   (`proxy_budget_canary.json` `runtime_fingerprint.files`), so this change
   invalidates all samples. It must land **before** the 120-sample
   re-collection, in the same fingerprint rotation as the phase-2 fix —
   otherwise ≈78 MiB of samples get burned twice.

Payoff on the immediate re-collection: t16 and t17 match classes merge into
one shape class, likewise player classes; season shapes stay separate
(calendar-year 48-team WC vs split-year 20-team EPL). Re-collection drops
from 6 classes ≈ 78.3 MiB to 4 classes ≈ 48.0 MiB
(20×1.199 + 20×0.317 + 2×20×0.442), and the merged classes automatically
satisfy the ≥2-tournament invariant. Onboarding LaLiga/Serie A then costs
**0 additional canary MiB** (same shapes) — but note this holds **only
because** `ESP-La Liga` and `ITA-Serie A` already have complete
`competitions.yaml` entries with sofascore in `sources.fallback`
(`competitions.yaml:181,250`), so no fingerprint input changes (§2.4); a
genuinely new league needs a `competitions.yaml` row and therefore rides a
batched rotation + re-collection. An 18-team league (Bundesliga, Ligue 1 —
also already present, `:320,391`) costs one new season-shape class
≈ 8.8 MiB if the team-count band separates it, or 0 if the band is 16–20.

### 3.4 Risks

- **Under-specified shape**: two leagues share a digest but differ in real
  byte cost (shotmap density, extra time in cups). Bounded by design — the
  proxy stops at the hard cap and the task fails loudly with evidence; the
  ≥2-tournament sampling invariant and team-count bands reduce the miss
  probability. A capped-out league becomes a new-shape candidate, never a
  guessed headroom.
- **Over-specified shape**: every league hashes unique and variant A decays
  into variant C. Mitigation: keep only byte-driving fields in the digest;
  team-count *bands*, not exact counts.
- **Schema/loader churn**: v3 loader must reject v2 artifacts as
  fail-closed migration input, mirroring how v1 is treated today
  (sofascore-proxy-budget.md, "Schema-v1 artifacts are migration input only").
- Rollback cost is high: schema + fingerprint + re-collection. This is the
  fork to decide first (section 6).

## 4. Positive adult-men evidence

The gate needs *positive* evidence of `{football, male, adult, first_team}`;
absence of "Women"/"U21" in a name proves nothing (`registry.py` module
docstring and `classify_tournament_source`). Options:

**(a) Dataset evidence from Bronze.** t16/t17 already carry
`{"type": "source_dataset", "endpoint": "bronze.sofascore_schedule",
"field": "home_gender/away_gender", "value": "M"}` rows in
`tournaments.json`. Strong and automatable — but circular for new leagues:
capture requires `enabled=true`, which requires approved review
(`set_activation` → `activation_eligibility.require()`,
`registry.py:341-348`). Useful **post-activation** as a standing DQ
invariant: any non-`'M'` `home_gender`/`away_gender` row for an enabled
league should alert/auto-disable. Not a bootstrap source.

**(b) Discovery payload fields.** Verified from the registry itself: the
live detail endpoint `/unique-tournament/{id}` returned `gender: "M"` for
both tournaments ever fetched live (evidence rows with
`"endpoint": "/unique-tournament/16"` and `"/unique-tournament/17"`), and
`classify_tournament_source` already consumes `gender`/`sex`,
`ageGroup`/`ageCategory`, `isYouth`, `teamLevel`, `isReserve`
(`registry.py:119-160`). Two important honesty notes: (1) the 10
`gender=unknown` rows have *never* been refreshed live — their unknown
status reflects a scan that never ran, not a confirmed source gap; (2) even
the live t16/t17 fetches left `age_group` and `team_level` `unknown`, so on
current evidence the source does **not** supply adult/first-team positives —
only gender. Catalog/category payload gender coverage is unproven (the test
fixture `_catalog_item` in
`tests/unit/scrapers/test_sofascore_discovery.py:50` assumes `"gender": "M"`
can appear, but that is a fixture, not a live observation). Consequence: a
per-tournament detail pass (the `active-reviewed` mechanics,
`discovery.py:1086-1111`, extended to review candidates) can automate the
**gender=male** half of the gate cheaply; adult/first-team cannot come from
the source today.

**(c) Cross-source correlation.** A tournament whose generated
`canonical_id` matches a `competitions.yaml` entry already served by
fbref/fotmob/whoscored inherits their scope context (the platform's other
pipelines are men's-adult only). This is exactly what the t16/t17 reviews
recorded (`"type": "repository", "reference":
"configs/medallion/competitions.yaml#ENG-Premier-League"`). Automatable as
*evidence generation*; the approval itself stays human.

**(d) Batch operator review.** Same review semantics as today
(`approve_tournament`, `registry.py:297-338` — source exclusions and unknown
gender are non-overridable), but N tournaments per invocation (section 5.2).

**Recommendation** — layered, keeping unknown-never-activates intact:

1. source `gender=male` from a live detail-endpoint discovery pass (b) —
   machine-collected, required, fail-closed;
2. operator batch approval (d) confirming adult/first-team, with
   cross-source references (c) pre-generated into the review evidence;
3. post-activation dataset DQ (a) as a tripwire that demotes a league if
   Bronze ever contradicts `'M'`.

No option softens `activation_eligibility`; the change is tooling that
manufactures evidence faster, not a weaker gate.

## 5. Auto-onboarding path: discovery → registry → competitions.yaml

### 5.1 Discovery transport (the broken first step)

Shipped discovery is direct-only by policy: `DirectSofaScoreClient` disables
proxies three independent ways (`trust_env=False`, `proxies={}`,
`CURLOPT_PROXY=""` — `discovery.py:288-331`), hardcodes
`paid_proxy_bytes: 0` in its stats (`:340-347`), and the workflow asserts
zero paid bytes and requires a self-hosted runner labelled
`sofascore-direct` (`.github/workflows/sofascore-discovery.yml:81,124-132`).
No such runner is currently registered (operational fact, not verifiable
from the repo). Measured egress facts:

| transport | egress | result |
| --- | --- | --- |
| any direct client | local host, GitHub-hosted Linux/macOS/Windows | HTTP 403, 48-byte body (sofascore-proxy-budget.md, 2026-07-11) |
| `tls_client` (chrome_120) | residential proxy exit | HTTP 200 (live diagnostic, 2026-07-12) |
| `requests` | residential proxy exit | HTTP 403 (ibid.) |
| `tls_client` | datacenter direct | HTTP 403 (ibid.) |

The edge requires a browser TLS fingerprint **and** a residential-grade exit
simultaneously (the shipped client pins `chrome_133`,
`discovery.py:233-236`). The 2026-07-12 diagnostics also measured: the
catalog+category fan-out alone ≈ 1,500 requests / ≈6 MiB of paid bytes,
discovering 6,059 tournaments; the full traversal was reported at ≈29 MB;
`--scope active-reviewed` = 5 requests / 26 KB. These live numbers are not
re-derivable from the repo. `discover_registry` with `scope="full"`
additionally fetches one season list per discovered tournament
(`discovery.py:1123-1151`), so a *complete* full scan scales to ≈7,500
requests — **budget a full scan as ≈6 MiB (fan-out lower bound, without the
6,059 season requests) to ≈29 MiB (complete-traversal upper bound)**. A
wholesale merge of 6,059 tournaments into `tournaments.json` is never to be
committed as-is; scope the merge to adult-men candidates. Discovery has no
built-in rate limiter (only retry backoff, `discovery.py:356-368`); through
a paid exit it should reuse the sofascore 20 req/min preset → ≈6 h for a
complete full scan, minutes for `active-reviewed`.

Fork — two viable transports (the zero-proxy rule is a deliberate policy;
this is a proposed *revision*, not a silent weakening):

1. **Self-hosted runner with residential egress.** Keeps the zero-paid-byte
   invariant and the existing PR workflow untouched. But "residential
   egress" means in practice a machine on a household ISP holding repo-write
   credentials — an availability and security liability, and the reason the
   runner still does not exist.
2. **Lease-proxy discovery (recommended).** A separate proxied discovery
   client (new source id, e.g. `sofascore_discovery`, alongside
   `sofascore`/`sofascore_canary` in `filter_proxy.py`) with an explicit
   owner-set cap per scan; JSON-only, no browser, metered like everything
   else. Price: ≈6–29 MiB per full scan (fan-out lower bound to
   complete-traversal upper bound — worth one metered pilot before fixing
   the cap), ~26 KB per daily refresh; a weekly full scan is ~6–29% of one
   day's shared budget. It can run as an Airflow DAG
   on the VM (which already holds the residential pool) and open the same
   review PR the workflow does today, removing the GitHub-runner dependency
   entirely. `DirectSofaScoreClient` stays as-is for environments with
   working direct egress; the proxied client is a sibling, not a fallback
   that silently kicks in.

### 5.2 Batch review and activation

`manage_sofascore_registry.py` handles one tournament per call. Proposed:
an `approve-batch` subcommand taking a reviewed JSON file
(`[{tournament_id, canonical_id, evidence: [...], notes}]`), applying
`approve_tournament` per row and writing once through the existing
compare-and-swap lock (`write_registry_atomic(..., expected_current=...)`,
`discovery.py:1231-1289`) — all-or-nothing, so a single bad row aborts the
batch before any operator decision is persisted. `enable` stays per-row and
re-runs `activation_eligibility` unchanged.

### 5.3 canonical_id generation and the competitions.yaml record

Convention observed in `competitions.yaml`: `"{PREFIX}-{Name}"` with
`PREFIX` ∈ {FIFA-style trigram: ENG/ESP/GER/ITA/FRA/…} or an organization
token (INT, UEFA). Generator: registry `category.slug` → prefix table +
tournament `name`, e.g. `(england, Premier League)` → `ENG-Premier League`.
Any collision with an existing `competitions.yaml` id, any non-mapped
category, and any prefix ambiguity is emitted for **manual confirmation** —
9 of 12 current registry rows already carry hand-set `canonical_id`s that
the generator must reproduce byte-for-byte as its own sanity check.

Auto-generated `competitions.yaml` entry (PR-flow identical to the
discovery workflow: automation branch + review PR, humans merge):

```yaml
- id: "<canonical_id>"
  name: "<registry name>"
  country: "<from category>"
  tier: 1                     # operator-confirmed
  seasons:
    - id: <canonical_season>  # from registry seasons
      format: "league_round_robin" | "group_knockout"
      team_count: <REQUIRED — not in the SofaScore registry>
      # match_count REQUIRED for group_knockout (competitions.yaml header)
      start: "<season start_date>"
      end: "<season end_date>"
  sources:
    fallback: ["sofascore"]   # promoted to primary after coverage evidence
  in_scope: false             # flipped when Gold materialization is decided
```

`team_count`/`match_count` cannot be auto-filled: registry seasons carry
ids, labels and dates but no participant counts. They come from the operator
(or from `standings_total`/`participants` after the first capture — too late
for the initial entry). The generator therefore emits them as explicit
`TODO` fields that block PR merge.

### 5.4 What stays manual forever — and why that is correct

- Final approval of adult/first-team status (source does not provide it,
  section 4b) and every registry `enable`.
- Merging discovery and competitions.yaml PRs.
- `verified: true` on the budget artifact and any paid-byte cap change —
  these are the platform's money- and content-safety authorization
  boundaries; the entire design is fail-closed *because* a human sits at
  them. Automation above only reduces the cost of preparing evidence, never
  the authority to act on it.

## 6. Cost/time plan

Stages, with paid bytes and calendar time (calendar figures assume a
20–30 MiB/day SofaScore share of the 100 MiB budget while TM/FotMob run):

| # | stage | paid cost | calendar | depends on |
| --- | --- | ---: | --- | --- |
| 0 | Phase-2 lease failover fix (in flight) — reduces the 12-of-80 canary run mortality; rotates the fingerprint | 0 | days | — |
| 1 | Owner decision on class keying (fork F1) **before** any re-collection | 0 | — | this doc |
| 2 | If variant A: schema v3 + loader + collector changes | 0 | ~1 week eng | 1 |
| 3 | Re-collect canary samples for t16+t17 | 78.3 MiB (status quo) or 48.0 MiB (merged shape classes) | 3–4 days; ≈8 h (or ≈5 h) of exclusive canary lock spread across them | 0, 2 |
| 4 | Offline `verify` → `verified: true` → weekend production for 2 tournaments | 0 | 1–2 days | 3 |
| 5 | Discovery transport (F3) + detail-endpoint gender pass + batch tooling (5.1–5.3) | ≈6–29 MiB per full scan under option 2 (§5.1); 0 under option 1 | 1–2 weeks eng/ops | parallel to 3–4 |
| 6 | Onboard wave 1: t8 LaLiga, t23 Serie A (EPL shape; already in competitions.yaml → no fingerprint rotation, §2.4) | variant A: 0 canary MiB; variant C: 2×39 MiB + full re-collection (collector edit rotates the fingerprint, §2.4) | days | 4, 5 |
| 7 | Onboard wave 2: t35 Bundesliga, t34 Ligue 1 (18-team shape, already in competitions.yaml) and further waves grouped by shape | variant A: ≈8.8 MiB per new season shape; **+ full re-collection (≈48 MiB) for any wave that edits competitions.yaml** — batch each wave's edits into one rotation (F2); variant C: 39 MiB per league + re-collection per rotation | per wave | 6 |
| 8 | Weekend production at scale + weekly-cadence scheduling change (season-phase gating, player rotation — phase 4 code) | steady state per section 2.2 | ongoing | 6+ |

### Forks

- **F1 — class keying**: (A) shape-digest classes / (B) donor-class
  attestation / (C) per-tournament status quo. Cost: A ≈ 1 week eng + 48 MiB
  re-collection; B ≈ 5.9 MiB/league but breaks max-observed purity; C ≈
  39 MiB and ~4 h lock per league. **Recommendation: A.** Expensive to roll
  back (schema + fingerprint + re-collection) — decide first.
- **F2 — sequencing and rotation batching**: land F1(A) inside the same
  fingerprint rotation as phase 2, before re-collecting — the alternative
  (re-collect now, migrate later) double-spends ≈78 MiB. Thereafter, batch
  every wave's `competitions.yaml` edits (new entries, yearly season rows,
  sources changes) into one rotation followed by one re-collection window
  (§2.4); unbatched edits each cost a full ≈48 MiB re-collection.
  **Recommendation: decide F1 before stage 3; adopt batched rotations as
  standing policy.** Cheap to state, expensive to violate.
- **F3 — discovery transport**: residential self-hosted runner vs metered
  lease-proxy discovery source (≈6–29 MiB per full scan, §5.1).
  **Recommendation: lease-proxy discovery as an explicit policy revision**;
  runner remains preferable only if the owner can actually provision trusted
  residential egress. Rollback: cheap either way (transport choice does not
  touch the budget artifact).
- **F4 — positive evidence**: detail-endpoint gender + batch review with
  cross-source references + post-capture dataset DQ (layered), vs
  dataset-only (circular), vs manual-only (does not scale).
  **Recommendation: layered**, gate semantics unchanged. Rollback: cheap.
- **F5 — player-phase cadence at scale**: weekly for all leagues (≈86 MiB
  every Saturday at 20 EPL-sized leagues, 20 × 4.292 — exceeds any realistic
  share) vs rotating players 1-in-4 weekends per league vs
  changed-players-only capture (phase-4 code). **Recommendation: rotation at
  >10 leagues**, revisit changed-only later. Rollback: cheap (scheduling
  parameter).
- **F6 — shared budget**: keep 100 MiB/day shared vs raise
  `PROXY_FILTER_DAILY_BUDGET_MB` vs introduce per-source quotas.
  **Recommendation: keep 100 and plan within a 30–50 MiB SofaScore share**
  until measured weekend actuals exist; revisit with data. Rollback: cheap.

## 7. Decision needed from the owner

1. F1: approve shape-keyed budget classes (variant A) — yes/no? If no,
   which of B/C, accepting its per-league cost?
2. F2: confirm ordering — phase-2 fix + schema v3 land together, *then* the
   ~48 MiB / ~5 h-lock re-collection for t16+t17 — and confirm batched
   fingerprint rotations (one rotation + one re-collection per
   `competitions.yaml` wave, §2.4) as standing policy?
3. F3: approve the discovery policy revision (metered lease-proxy discovery
   source, ≈6–29 MiB per full scan, cap set by you) — or will you provision
   a residential self-hosted runner instead?
4. F4: approve the layered evidence scheme, specifically that source
   `gender=M` (machine) + your batch approval of adult/first-team (human)
   is sufficient positive evidence, with the Bronze `'M'` tripwire?
5. Target scale and share: how many leagues in wave 1/2, and what SofaScore
   daily share (30/50/70 MiB) should capacity planning assume while
   TM/FotMob backfills run?
6. F5: player-phase rotation acceptable once league count exceeds ~10?
7. Budget authorization for stage 3 (re-collection, ≈48 MiB paid) and
   stage 5 pilot (≈6–12 MiB paid if F3 = lease-proxy) — explicitly, since
   the standing rule forbids re-collection without separate permission.
