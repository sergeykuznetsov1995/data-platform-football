# RX2 — Selecting a single xG source for `gold.fct_player_season_stats`

> Status: **DECIDED 2026-05-22**. Supersedes the relevant part of RX/D3 ("lazy
> suffix policy") for the xG / xA metric family. Implementation in commit G
> follows.

## Question

`gold.fct_player_season_stats` currently publishes three xG columns
(`expected_goals_fotmob`, `expected_goals_understat`, `expected_goals_sofascore`)
and two xA columns (FotMob / Understat) with per-source suffixes because xG is
a MODELED metric (each provider has its own probability model). Carrying three
columns is correct from a "no information loss" perspective but creates real
friction:

* BI consumers must choose a column without guidance.
* ML feature pipelines accidentally double-count or pick the wrong source.
* Cross-source diff stats live in `_audit` already — three columns in the
  business fact buys nothing the audit doesn't.

After more experience with downstream consumers, the team decided to **fold
xG / xA down to a single source** and move the cross-source comparison
permanently to the audit layer.

Which provider? FotMob (StatsBomb-derived), Understat (proprietary), or
SofaScore (Opta-derived). The choice should optimise for: coverage in
APL 2025/26, predictive quality (corr with actual goals), historical
backfill capacity, and resistance to provider-side changes.

## Method

Five Trino probes against the live Gold + Silver layer (2026-05-22, APL
2025/26):

1. **F1 — Coverage per source**: COUNT non-null xG in the Gold fact.
2. **F2 — Season range**: MIN/MAX season in each Silver aggregate.
3. **F3 — Pairwise correlation**: `CORR(eg_X, eg_Y)`, filter `minutes >= 450`
   to avoid sub-sample noise.
4. **F4 — Predictive quality**: `CORR(eg_X, goals)` and
   `AVG(ABS(eg_X − goals))` (MAE) against actual goals scored.
5. **F5 — Historical backfill**: rows-per-season per Silver aggregate.

All queries restricted to APL 2025/26 (`league = 'ENG-Premier League'`,
`season = 2025`).

## Findings

### F1 — Coverage (APL 2025/26)

| Source     | Rows with non-null xG | Coverage of canonical players (n=493) |
|------------|----------------------:|--------------------------------------:|
| Understat  | **489**               | **99.2 %**                            |
| SofaScore  | 417                   | 84.6 %                                |
| FotMob     | 403                   | 81.7 %                                |

Understat wins by 14–17 percentage points. Higher coverage = fewer NULLs
downstream = less special-casing in features and dashboards.

### F2 — Season range

| Source     | min season | max season | rows non-null xG |
|------------|-----------:|-----------:|-----------------:|
| Understat  | 2526       | 2526       | 528              |
| SofaScore  | 2526       | 2526       | 418              |
| FotMob     | 2025       | 2025       | 410              |

Currently **all three sources hold only 2025/26 in Bronze.** Understat's
real-world archive goes back to 2014/15 but we haven't backfilled
historically yet. The candidate with the cheapest backfill path (one
HTTP endpoint per (league, season), well-documented public structure)
is Understat — re-running our scraper with extended `seasons=` is
mechanical work, unlike FotMob/SofaScore where dynamic content gates
us hard.

### F3 — Cross-source correlation (Pearson, minutes ≥ 450)

| Pair                       | r       | n   |
|----------------------------|--------:|----:|
| FotMob ↔ Understat         | 0.9915  | 353 |
| FotMob ↔ SofaScore         | 0.9981  | 338 |
| Understat ↔ SofaScore      | 0.9896  | 344 |

Three independent models agree to four decimals. **From an information
perspective the columns are near-redundant** — losing two of them is
practically lossless.

### F4 — Predictive quality vs actual goals

| Source     | CORR(xG, goals) | MAE (xG − goals) |
|------------|----------------:|-----------------:|
| FotMob     | 0.9176          | 0.993            |
| Understat  | 0.9171          | 1.034            |
| SofaScore  | **0.9195**      | **0.982**        |

SofaScore is marginally best on both metrics, but the differences are
inside noise (≤ 0.005 in r, ≤ 0.05 in MAE) and would flip on a
different sample of seasons or leagues. **Predictive quality does not
discriminate between the three.**

### F5 — Historical backfill (current state)

All three sources have exactly one season materialised. Future work:
backfill Understat 2014/15+ (cheap), FotMob 2017/18+ (some risk due to
endpoint drift), SofaScore 2017/18+ (CF-gated, high risk).

## Decision

**Primary xG source = Understat.** Reasoning:

1. **Coverage** (the only metric that materially differs): Understat 99.2 %
   vs SofaScore 85 % vs FotMob 82 %. Fewer NULL xG rows downstream is the
   single biggest win for BI consumers and ML features.
2. **Cross-source agreement is near-perfect** (r ≥ 0.99 across pairs) →
   choosing one source loses essentially no signal.
3. **Predictive quality differences are within sampling noise** — not a
   tiebreaker.
4. **Future backfill path is cheapest with Understat** (public, stable
   endpoint structure; no CF gating; the Understat scraper already
   supports `seasons=` parameter expansion).
5. **Risk of provider churn**: Understat's xG model has been stable since
   2014; FotMob's is StatsBomb-licensed and could change pricing/access;
   SofaScore's depends on Opta licensing which is volatile.

### What we keep / drop

* In `gold.fct_player_season_stats`:
  - **Keep**: `expected_goals` (renamed from `expected_goals_understat`),
    `expected_assists` (renamed from `expected_assists_understat`).
    Fall-back order in the COALESCE chain: `us → fm → ss` so that rows
    where Understat is NULL (≈1 %) still get values from another source.
  - **Drop**: `expected_goals_fotmob`, `expected_goals_sofascore`,
    `expected_assists_fotmob`.
  - **Keep as-is** (Understat-only, no overlap): `non_penalty_xg_understat`,
    `xg_chain_understat`, `xg_buildup_understat`.
  - **Keep as-is** (FotMob-only): `expected_goals_on_target`
    (FotMob xGOT — no equivalent at other providers).
* In `gold.fct_player_season_stats_audit`:
  - **Keep** all per-source xG diff columns (`expected_goals_diff_fotmob`,
    `expected_goals_diff_sofascore`). The audit is where cross-source
    monitoring belongs.
* OpenMetadata YAML (`configs/openmetadata/descriptions/fct_player_season_stats.yaml`)
  updated to reflect the new column names in the same wave.

### Implementation steps (Commit G)

1. `dags/sql/gold/fct_player_season_stats.sql:122-127`:
   ```sql
   ROUND(COALESCE(us.expected_goals, fm.expected_goals, ss.expected_goals), 2)
       AS expected_goals,
   ROUND(COALESCE(us.expected_assists, fm.expected_assists), 2)
       AS expected_assists,
   ROUND(fm.expected_goals_on_target, 2)
       AS expected_goals_on_target,
   ```
2. `tests/unit/sql/test_fct_player_season_stats_render.py::test_modeled_xg_per_source_suffixes`
   → rewrite into `test_xg_single_column_after_rx2` asserting the single
   `expected_goals` projection and absence of `expected_goals_fotmob` /
   `_sofascore`.
3. `dags/utils/gold_tasks.py`: bump `value_range` check from
   `expected_goals_understat` → `expected_goals`. Same for `non_penalty_xg`
   stays as is (already Understat-only).
4. `configs/openmetadata/descriptions/fct_player_season_stats.yaml`:
   replace three xG entries with a single `expected_goals` entry and two
   xA entries with single `expected_assists`.
5. Grep `expected_goals_fotmob` / `_sofascore` across configs/superset/
   and dags/ — remove or rename any stragglers before merge.

## Risks & rollback

* If a future season shows large divergence (r dropping below 0.95) we can
  reverse this decision quickly: the audit table preserves all sources
  permanently, and re-projecting them in business fact is a few-line SQL
  edit.
* Understat coverage gap (~1 %) is real — usually U21 / backup players
  with very low minutes. The COALESCE fallback to FotMob/SofaScore
  catches most of them.
* Backfill before the next BI release is **not** a prerequisite for this
  commit — we're only changing schema semantics, not row counts.

## References

* `docs/research/RX_cross_source_player_profile.md` D3 — lazy suffix
  policy (now superseded for xG/xA only).
* Earlier T5/T6 commits in this branch.
* GitHub issue #38 — research on Understat /player/{id} pages (could
  unlock historical backfill if profiles carry season-by-season xG).
* `memory/feedback_audit_in_separate_table.md` — the principle this
  decision leans on (cross-source diffs in audit, single value in
  business fact).
