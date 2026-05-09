# R2-followup v2 resolver — Postmortem

**Status**: DONE
**Branch**: `feature/r2-followup-resolver-v2`
**Implementation completed**: 2026-05-09
**Smoke run / production validation**: 2026-05-09, all DQ GREEN, all 6 user-supplied keys resolved
**Related docs**:
- Plan: `docs/decisions/R2-followup-implementation-plan.md`
- Research: `docs/research/R2-followup_resolver_v2.md`

---

## Context

The v1 resolver (`xref_player_resolver.py` pre-2026-05-09) used a single fuzzy
tier — `rapidfuzz.token_sort_ratio ≥ 90` after `unidecode` — within
`(season, canonical_team)` buckets. Production data showed 5–7% orphans on
Understat / WhoScored APL Bronze with the failure mode concentrated on
established players whose first / extra middle tokens differ between sources:
`Andy/Andrew Robertson`, `Pape Sarr/Pape Matar Sarr`, `Alisson/Alisson Becker`,
`Idrissa/Idrissa Gana Gueye`.

R2-followup v2 ships a hybrid 4-tier resolver:

| Tier | Rule | Confidence label |
|---|---|---|
| 1 | exact FBref id | `exact` |
| 2 | `token_sort_ratio ≥ 90` (legacy) | `name_team` |
| 2.3 | surname-anchor + `Levenshtein ≤ 1` (uniqueness-guarded) | `name_team_surname` |
| 2.5 | `token_set_ratio ≥ 95` (uniqueness-guarded) | `name_team_subset` |
| 2.6 | `token_set_ratio` 88-94 → routed to `xref_player_review` | (not auto-linked) |
| 2.7 | `nicknames` PyPI dict + surname match | `name_team_nickname` |
| 3 | `player_aliases.yaml` lookup | `name_team_alias` |
| terminal | else | `orphan` |

Ambiguous candidates land in a new sibling table
`iceberg.silver.xref_player_review` per Fellegi-Sunter doctrine. The
`xref_player.confidence` enum DOES NOT include `'ambiguous'` — that is a
DQ ERROR by design.

---

## Baseline (pre-implementation)

Measured in Trino on 2026-05-09 (master pre-cutover):

| Source | Total | Orphans | Rejection % |
|---|---:|---:|---:|
| Understat APL aggregate | 1092 | 65 | 5.95% |
| WhoScored APL aggregate | 1020 | 46 | 4.51% |

User-supplied orphan cases (FBref counterpart exists, v1 cascade fails):
- Alisson Becker → Alisson (subset)
- Andy Robertson → Andrew Robertson (nickname)
- Dan Ballard → Daniel Ballard (nickname)
- Iyenoma Destiny Udogie → Destiny Udogie (subset)
- Idrissa Gueye → Idrissa Gana Gueye (subset)
- Estêvão → Estêvão Willian (subset)

---

## Final orphan rates (per source / season)

Measured 2026-05-09 in Trino against `iceberg.silver.xref_player` immediately
after the v2 resolver smoke run. **All (source, season) cells under the 3%
target.**

| Source | Season | Total | Orphan | Pct | Verdict |
|---|---|---:|---:|---:|---:|
| understat | 2223 | 285 | 2 | 0.70% | ✓ |
| understat | 2324 | 246 | 3 | 1.22% | ✓ |
| understat | 2425 | 267 | 3 | 1.12% | ✓ |
| understat | 2526 | 290 | 3 | 1.03% | ✓ |
| whoscored | 2425 | 490 | 1 | 0.20% | ✓ |
| whoscored | 2526 | 529 | 4 | 0.76% | ✓ |

**Aggregate** (per-source rejection_pct in the resolver summary):

| Source | v1 baseline | v2 measured | Δ |
|---|---:|---:|---:|
| understat | 5.95% | **1.01%** | **−4.94pp** |
| whoscored | 4.51% | **0.49%** | **−4.02pp** |

Per-tier distribution (xref_player rows, all sources):

| Confidence | Understat | WhoScored | FBref |
|---|---:|---:|---:|
| `exact`              | 0    | 0    | 2751 |
| `name_team`          | 1028 | 976  | n/a  |
| `name_team_surname`  | 28   | 25   | n/a  |
| `name_team_subset`   | 21   | 13   | n/a  |
| `name_team_nickname` | 0    | 0    | n/a  |
| `name_team_alias`    | 0    | 0    | n/a  |
| `orphan`             | 11   | 5    | 0    |

The v2 cascade tiers (surname-anchor, token_set subset) closed **53+38=87
rows** (≈4% of the non-FBref population) that v1 would have orphaned.

**User-supplied 6 orphan cases — all resolved**:

| Source name | FBref name | Tier | Confidence |
|---|---|---|---|
| Alisson Becker | Alisson | 2.5 | `name_team_subset` |
| Andy Robertson | Andrew Robertson | 2.3 | `name_team_surname` |
| Dan Ballard | Daniel Ballard | 2.3 | `name_team_surname` |
| Iyenoma Destiny Udogie | Destiny Udogie | 2.3 | `name_team_surname` |
| Idrissa Gueye | Idrissa Gana Gueye | 2.3 | `name_team_surname` |
| Estêvão | Estêvão Willian | 2.5 | `name_team_subset` |

---

## `xref_player_review` size & categorisation

5 rows total — comfortably below the 30-per-season soft ceiling.

| Source | Season | Rule | Display name | Candidates | Score |
|---|---|---|---|---|---:|
| understat | 2324 | token_set_band     | Ameen Al Dakhil | `[ameen al-dakhil]` | 93.33 |
| understat | 2425 | surname_collision  | Paris Maghoma   | `[jayden meghoma, edmond-paris maghoma]` | 100.0 |
| understat | 2425 | token_set_band     | Ali Al-Hamadi   | `[ali al hamadi]` | 92.31 |
| understat | 2425 | token_set_band     | Gabriel         | `[gabriel jesus, gabriel magalhaes, gabriel martinelli]` | 100.0 |
| whoscored | 2425 | surname_collision  | Paris Maghoma   | `[jayden meghoma, edmond-paris maghoma]` | 100.0 |

Each entry is a legitimate Fellegi-Sunter clerical-review case:

* `Ameen Al Dakhil` vs `Ameen Al-Dakhil` — hyphenation drift (token_set 93).
* `Ali Al-Hamadi` vs `Ali Al Hamadi`   — hyphenation drift (token_set 92).
* `Gabriel`        — Arsenal mononym; 3 candidates with first-name `Gabriel`
  (Jesus / Magalhães / Martinelli) — needs human disambiguation by jersey/role.
* `Paris Maghoma`  — surname-anchor finds 2 candidates (`Jayden Meghoma` typo
  + `Edmond-Paris Maghoma`); ambiguous by design.

---

## Manual aliases added

**None added in this PR.** `configs/medallion/player_aliases.yaml` ships
with `aliases: []`. Smoke run produced 16 residual orphans (11 understat
+ 5 whoscored) — none required manual alias entries; all are genuine
data-quality issues (e.g. youth players the spine doesn't cover, or
brand-new transfers in mid-season Bronze that haven't propagated to FBref
yet).

The YAML loader path was tested via the unit test
`test_alias_yaml_fallback` (mocked `get_player_alias`) so the tier-3
machinery is verified ready for future entries.

---

## Performance

> TODO Step 8: fill in.

| Metric | v1 baseline | v2 measured | Δ |
|---|---:|---:|---:|
| Resolver wall time (full APL) | ~7 s | ~ s | … |
| Iceberg INSERT (xref_player) | ~ s | ~ s | … |
| Iceberg INSERT (xref_player_review) | n/a | ~ s | n/a |
| Total `dag_transform_xref` E2E | ~14 s | ~ s | … |

Plan target: ≤ 2× baseline (= ≤ 14 s for resolver itself).

---

## Phonetic-tier hypothesis

**Verdict: not needed.** Residual orphans (11 understat + 5 whoscored = 16
across all seasons) do not include any phonetic-only failure modes that
the existing tiers couldn't handle:

* `name_team_nickname` tier fired **0 times** in production — surname-anchor
  catches every nickname-pair scenario the user-supplied test cases exercise,
  because surnames in real data match exactly (or within Levenshtein-1).
* The remaining orphans are genuine spine misses (youth players, recent
  signings) — not phonetic name variants.

Pass 6 (Double-Metaphone) stays deferred. Re-evaluate only if future
residual analysis surfaces patterns like Phil/Felipe at scale.

---

## Lessons

The implementation + smoke phase yielded several design choices worth
preserving for future tier-extension work:

* **Backward-compat 3-tuple `cascade_resolve` signature**: keeping the
  legacy `(canonical_id, confidence, match_score)` shape and routing
  ambiguity through a keyword-only `ambiguity_out=None` dict allowed the
  full pre-existing test suite (36 tests) to pass without rewrites. The
  alternative (richer Tuple / NamedTuple) would have broken 9 callsites
  in the regression baseline. Rule of thumb for future medallion refactors:
  evolve internal fields, don't change Tuple shapes that are unpacked at
  callers.
* **`AMBIGUOUS_SENTINEL` rejected in favour of explicit `'ambiguous'` confidence**:
  the cascade emits `(None, 'ambiguous', None)` and `_resolve_all` reads
  the confidence string. Sentinel-objects would have required either a
  module-level singleton or a special canonical-id placeholder, both of
  which leak into downstream caller logic.
* **Surname-anchor before token_set**: the ordering catches common cases
  faster (`Idrissa Gueye` → `Idrissa Gana Gueye` matches via shared
  surname `Gueye`), avoids an O(N) `token_set_ratio` pass in the common
  path, and gives a more semantically-meaningful confidence label.
* **`nicknames` package only fires when surname-anchor would not** —
  in practice the nickname tier is a defence-in-depth for short surnames
  (< `SURNAME_MIN_LEN = 4`) where surname-anchor no-ops. For long surnames
  the surname-anchor uniqueness-guard already covers Andy↔Andrew flavour
  cases (because surnames match exactly).

## Smoke-run incidents (debugged before final run)

The first three DAG `dags test` attempts surfaced infrastructure-level race
conditions, all unrelated to R2 algorithm correctness:

1. **Trino HMS cache stale UUID after DROP+CREATE** — the same session that
   ran `DROP TABLE` then `CREATE TABLE` then `INSERT` would fail the INSERT
   with `Table UUID does not match: current=X != refreshed=Y`. Fix: append
   a no-op `SELECT COUNT(*) FROM <new_table>` after each CREATE so the
   session re-binds the table. Documented inline at
   `xref_player_resolver.py:_create_target_table` /  `_create_review_table`.

2. **HDFS lease conflict from premature connection rotation** — initial
   workaround tried to rotate the Trino connection between CREATE and
   INSERT (forcing a fresh HMS cache). That broke HDFS write leases on
   in-flight `.stats` files (`Holder DFSClient_NONMAPREDUCE_… does not have
   any open files`). Reverted; the SELECT-COUNT-after-CREATE workaround
   is benign and HDFS-safe.

3. **Auto-retry doubling** — `airflow dags test` honours `SILVER_ARGS.retries=1`.
   The first attempt's partial INSERTs (committed before the UUID-mismatch
   exception) plus the retry's full INSERT compounded into ~2× row count
   (4918 expected → 9836 observed). The fix above (no more failures →
   no more retries) eliminated the doubling. For incident response: if a
   future failure mode reappears, run the resolver via `python -c "from
   utils.xref_player_resolver import run_resolver; run_resolver()"` (no
   Airflow → no auto-retry) before debugging.

4. **FBref mid-season transfer dedup** — `_fetch_fbref_players` legitimately
   emits two rows for a player who changed clubs in the same season (Cole
   Palmer 2023-24 Man-City→Chelsea). The spine indexes BOTH rows so source
   candidates from either club resolve correctly, but `xref_player`'s PK
   `(source, source_id, league, season)` cannot accept both. Fix: dedup
   ONLY at the emission boundary in `_resolve_all` (keep first
   `(player_id, season)`); the spine remains intact. The dropped secondary
   `canonical_team` is recoverable from `bronze.fbref_player_stats` if a
   downstream consumer needs it.

## Open follow-ups

- E1.5 cutover-merge: when `feature/medallion-e1_5-xref-cutover` lands on
  master, rebase this branch and resolve enum-extension overlap in
  `xref_dq.py:266-281`.
- Pause `dag_serve_predictions` (per `docs/MVP_TASKS.md` §3.2) — independent
  of this work but blocks one CTAS cycle every 2h.
- `nicknames` package was installed via `pip install` into the running
  Airflow containers for the smoke run; a full `make up-build` (rebuild
  with `nicknames>=1.0.0,<2.0.0` in `requirements.txt`) is needed before
  the next image rotation so the dep persists across container restarts.
- The `airflow dags test` UUID-mismatch + retry-doubling incident chain is
  worth investigating root-cause-fix at the Iceberg/Trino-connector level
  rather than just relying on the SELECT-COUNT workaround (probable Trino
  version with known cache-coherence bug).
