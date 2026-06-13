# Season-fact audit tables stay inline `.sql`

> Status: **active** · Created 2026-06-13 · Owner: data-platform
> Scope: `gold.fct_{player,team,keeper}_season_stats_audit`
> Decides: issue #556 (followup of #542 / PR #555)
> Enforced by: `tests/unit/sql/test_source_priority.py::test_season_audit_stays_inline`,
> `tests/unit/sql/test_fct_team_season_stats_inline.py::TestAuditCteSync`

## Decision

PR #555 (#542) moved the three **main** season facts onto
`configs/medallion/source_priority.yaml` + `.sql.j2` rendering. Their `_audit` siblings —
`fct_player_season_stats_audit.sql`, `fct_team_season_stats_audit.sql`,
`fct_keeper_season_stats_audit.sql` — **stay plain `.sql` and are NOT migrated.**

This is deliberate, not an oversight.

## Why audit cannot ride `source_priority.yaml`

The two are different abstractions:

| | `source_priority.yaml` (main facts) | audit tables |
|---|---|---|
| Question answered | "which source do we trust **first** for the merged column?" | "which **pairs** of sources do we diff for DQ?" |
| Emitter output | one `<wrap>COALESCE(s1, s2, …) AS <alias>,` per metric | many per-source diff columns per metric |
| Sources referenced | the metric's priority list | curated pairs, often **outside** that list |

1. **The emitter only knows single-COALESCE.** `get_source_priority_exprs()`
   (`dags/utils/medallion_config.py`) returns `Dict[str, str]` — exactly one merged SELECT
   line per metric, e.g.:

   ```
   m_goals → "CAST(COALESCE(fb.goals, fm.goals, us.goals) AS BIGINT) AS goals,"
   ```

2. **Audit is a per-source diff layout.** It emits a *matrix* of pairwise differences, not a
   merge:

   ```sql
   (CAST(fb.goals AS DOUBLE) - CAST(fm.goals AS DOUBLE)) AS goals_diff_fotmob,
   (CAST(fb.goals AS DOUBLE) - CAST(ss.goals AS DOUBLE)) AS goals_diff_sofascore,
   ROUND(CAST(us.xg AS DOUBLE) - CAST(ss.expected_goals AS DOUBLE), 4) AS xg_diff_us_vs_ss,
   ```

3. **Audit references sources outside the priority list.** `goals_diff_sofascore` exists
   even though SofaScore is not in the `goals` priority (`fb → fm → us`). Audit reaches for
   per-source aggregate column names (`ss.goals_for`, `ws.shots_on_target_proxy`,
   `us.games_played`) that are not metric aliases at all.

Expressing this through the YAML would require a **second, different emitter** (return
per-source expression *lists*, invent new placeholder syntax, encode which pairs to diff).
That is net-new infrastructure for a **DQ tool** whose golden output is ≈0 rows of
disagreement — not a business mart. *Simplicity first:* the cost has no matching value.

## Residual risk and its mitigation

`fct_team_season_stats_audit.sql` carries **truncated copies** of the main file's inline
per-source rollup CTEs (`us_team_season`, `ws_season_rollup`, `fm_team_season`) — a
consequence of dropping the derived gold floor in #478. The header warns
`⚠️ Синхронизировать вручную`. To make that warning enforceable,
`TestAuditCteSync` parses both files with sqlglot and asserts the audit CTEs' shared columns
are expression-identical to the main file. Drift now fails CI instead of rotting silently.

`ss_team_season` is **excluded** from that guard: the main file builds SofaScore in two
stages (`ss_match_rollup` → `ss_team_season`) while the audit fuses them into one stage, so
there is no line-by-line correspondence to compare.

## Alternatives considered

- **Shared SQL fragment** included by both main and audit (eliminating the copy outright).
  Rejected: the render mechanism is placeholder *substitution*, not an `include`/partial
  system — adding one is net-new infra and out of scope for #556. Worth revisiting only if
  the inline CTEs grow further.
