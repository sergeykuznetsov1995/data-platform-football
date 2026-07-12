# FBref production backlog matrix

Audit date: 2026-07-11

Code baseline: `origin/master` at `9eedd24783c6bb917941082385d685f841e19879`

Implementation branch: `feat/fbref-production-ready`

This is an evidence matrix for the current checkout. It does not change issue,
pull-request, label, comment, or Project state on GitHub. GitHub Issues and PRs
were readable, but GitHub Projects were not: the available credential lacks
the `read:project` scope. Project membership and Project-field status therefore
remain unavailable.

Status meanings:

- **implemented (code)**: reachable production code and focused regression
  coverage exist; live rollout may still be pending.
- **partial**: useful code exists, but the issue's full acceptance contract is
  not met.
- **blocker**: promotion must fail closed until the named evidence/data gap is
  resolved.
- **preserved**: the baseline implementation was already correct and remains
  protected by compatibility tests.

| Task | Baseline and issue evidence | Status in this checkout | Remaining evidence or blocker |
|---|---|---|---|
| [#923](https://github.com/sergeykuznetsov1995/data-platform-football/issues/923) | PR #924 added raw replay for match pages. PR #928 added standalone index→history→season→schedule discovery, but not a production Airflow path or all-page ingestion. | **implemented (code), production validation pending** | The production DAG now uses the source-discovered registry, PostgreSQL frontier/cohorts, fenced leases, exact hard budgets, raw-v2 commit-before-parse, per-observation generic/typed/stateful fences, independent typed dataset availability, offline replay, and fail-closed promotion. Production reuse is limited to exact-logical-refresh crash recovery; recurring-to-one-shot transitions make a fresh final fetch. The bounded canary stayed within 19 requests/1.68 MiB kernel-observed traffic but failed on classified warm HTTP `500`; no retry or unapproved 400-page soak ran. |
| [#901](https://github.com/sergeykuznetsov1995/data-platform-football/issues/901) | Latest issue evidence supersedes the body: 17,953 matches agree; 11 matches with recorded scores have no event rows (7 have a nonzero score); a separate 7 are awarded; 2 are shoot-outs; Strasbourg `94889482` is the true event mismatch. | **partial / blocker** | Event phases, `team_side`, on-field score, shoot-out separation, timeline `SO`, and strict DQ are implemented. The strict `scored_match_without_events` gate correctly blocks on the 11 raw event gaps. Repair must come from immutable raw replay or a separately approved bounded refetch; no events are synthesized. |
| [#902](https://github.com/sergeykuznetsov1995/data-platform-football/issues/902) | PR #914 exposed source score, event aggregate, notes, and `is_awarded`, but did not provide a separate authoritative result contract. | **implemented (code), data validation pending** | Legacy `home_score`/`away_score` retain FBref source semantics. Additive source/on-field/official/shoot-out fields, authority/reference/provenance, finite evidence-backed overrides, and a fail-closed missing-override DQ gate are present. Production Trino validation remains part of rollout. |
| [#916](https://github.com/sergeykuznetsov1995/data-platform-football/issues/916) | Commit `36d8a3c` made DQ green by dropping blank-ID rows such as Borello, Spizzichino, and Rutjens. | **implemented (code), production validation pending** | `silver.fbref_player_identity` unions player-bearing datasets, preserves source IDs, recovers only unique exact native IDs, and emits deterministic `noid_` IDs only for residuals. Season profiles consume the identity layer instead of filtering blank IDs. Live row-retention/xref coverage must still be measured. |
| [#870](https://github.com/sergeykuznetsov1995/data-platform-football/issues/870) | `fbref_match_keeper_stats` was produced from match HTML but had no registered schema, typed consumer, lineage, or Gold metrics. | **implemented (code)** | Bronze schema and metadata, `silver.fbref_keeper_match_stats`, strict PK/range/ref-integrity DQ, and nullable goalkeeper metrics in `gold.fct_player_match` are wired. Production data validation is pending with the main rollout. |
| [#926](https://github.com/sergeykuznetsov1995/data-platform-football/issues/926) | `_fetch_fbref_players()` read only `bronze.fbref_player_stats`; most documented starter orphans were keepers. | **implemented (code), production validation pending** | The identity universe includes season stats, match stats, lineups, and keeper rows; xref reads `silver.fbref_player_identity`. Starter-orphan and synthetic-residual rates still require production evidence. |
| [#903](https://github.com/sergeykuznetsov1995/data-platform-football/issues/903) | PR #918 reported five-league Gold rollout with 18,075 `dim_match` rows and zero null team IDs/FK orphans. | **preserved** | Existing Gold mapping and FK gates remain. No repeat rollout is claimed. |
| [#904](https://github.com/sergeykuznetsov1995/data-platform-football/issues/904) | PR #918 populated configured Top-5 competition/season mappings. | **preserved** | Mappings remain compatibility projections only and are not used as FBref crawl scope. |
| [#905](https://github.com/sergeykuznetsov1995/data-platform-football/issues/905) | PR #918 added 129 Top-5 team aliases. | **preserved** | Alias coverage remains regression-protected. |
| [#906](https://github.com/sergeykuznetsov1995/data-platform-football/issues/906) | PR #918 added referee aliases; manager/venue and non-EPL identity anchors remained outside that delivery. | **partial** | This checkout does not claim a complete evidence-backed manager/venue alias audit. Fuzzy guesses are still prohibited. |
| [#907](https://github.com/sergeykuznetsov1995/data-platform-football/issues/907) | Mandatory `competition_scope` landed via #913, while the old FBref runner still used EPL/World-Cup configuration. | **implemented for FBref ingestion** | The current DAG has no league parameter or manual FBref allowlist; registry gender eligibility is applied before frontier expansion. Other sources retain their own configuration and are outside this FBref scope. The failed canary did not reach registry parsing, so production registry coverage still requires a successful bounded rerun. |
| [#908](https://github.com/sergeykuznetsov1995/data-platform-football/issues/908) | Direct ERROR gates for `dim_match.league→dim_competition` and `season→dim_season` predated the issue and PR #918 removed stale rows. | **preserved** | Existing FK gates remain; no duplicate implementation was added. |

## Related classifications

- #892 / PR #912: **preserved** Bronze-hygiene precursor.
- #898: **obsolete/corrected** by the source/on-field/official score contract;
  its motivating match was awarded.
- #839 / PR #843: **preserved for the old EPL unused-sub floor**; it is not a
  substitute for the #916/#926 identity work.
- #913 and #920 with PRs #922/#925: **preserved for configured single-year
  World Cup compatibility**. Source-wide FBref scope now comes from discovery.
- #847 / PR #858: **preserved** historical Trino OOM/retry fix; not a current
  FBref production blocker.
- Historical transport/traffic issues #44, #45, #52, #57, #65, #116, #117,
  #124, #131, #616, #624, #877, and #893 are closed. Their traffic-accounting,
  resource-blocking, warm-session, and fail-closed lessons are retained.
- PR #611 is stale docs-only research and is absent from the baseline.
- The previously untracked consolidation work is now **implemented (code)**:
  the production DAG no longer uses `scrapers/nodriver_fbref`, legacy runner
  factories, or the filesystem/S3 discovery queue; unknown tables are captured
  by lossless generic Bronze instead of being silently dropped.

## Completion rule

An issue marked implemented here is not automatically production validated.
Production validation additionally requires a successful bounded live canary, green
strict Silver/xref/Gold promotion, traffic/performance evidence, and a final
non-overlapping regression run. Current readiness and blockers are recorded in
[`FBREF_PRODUCTION_READINESS_2026-07-11.md`](FBREF_PRODUCTION_READINESS_2026-07-11.md).
