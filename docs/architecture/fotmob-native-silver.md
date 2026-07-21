# FotMob native → Silver contract

Native Bronze keeps FotMob source identity as
`(competition_id, source_season_key)`. Silver preserves the historical
`(league, season)` consumer contract, but derives it only at the cutover
boundary:

- `competition_id → league` comes from
  `configs/fotmob/competitions.json` and is rendered into every FotMob/xref SQL
  consumer; the inner join is also the explicit 14-league compatibility scope;
- `season_year` is the first four characters of the exact source season key;
- World Cup keeps the four-digit year, while the other compatibility leagues
  keep the existing `YYZZ` season slug;
- global squad/player/team snapshots never manufacture history. Squad rows are
  joined only to a source-selected/latest season; historical player membership
  comes from season-grained leaderboards;
- coach identity is driven by current squad rows with `member_type='coach'`.
  Player snapshots are optional attribute fallback and cannot create or remove
  a manager identity.

The executable framework lives in the `league_map`, `season_axis`,
`team_scope`, `squad_scope`, `player_scope`, and `coach_scope` CTEs of the
profile/xref SQL. Contract tests enforce the shared expressions and registry
rendering.

`dag_ingest_fotmob` waits for `dag_transform_fotmob_silver` to finish before
the scheduled SofaScore production orchestrator may start xref. Xref, E3, E4
and FBref Gold each validate the same exact claimed publication generation
before their first write. This ordering is required because xref player and
its DQ now consume native-backed FotMob Silver profiles.

The 14:00 UTC FotMob trigger has exactly one owner. In the admitted production
state the shared master is paused, the scheduled SofaScore pipeline is
unpaused, and shared FotMob ingest/Silver are paused. SofaScore waits for and
claims the exact isolated generation before xref, then publishes or abandons it
only after E4 is terminal. The dormant shared master still permits its FotMob
trigger only when Airflow Variable `fotmob_schedule_owner` (or the
`FOTMOB_SCHEDULE_OWNER` environment fallback) is `shared`; isolated admission
requires a reviewed handoff to `isolated`. The isolated daily DAG is created
only under exact `FOTMOB_ISOLATED_STACK=1`; the shared default creates no such
DAG, and a stale shared DagModel is safe only while paused and quiescent.
Unknown values, stale serialized
fences and owner/pause/run state mismatches fail closed. Admission and live
operational checks require zero queued/running shared runs across master,
SofaScore, ingest, Silver, xref, E3, E4, FBref Gold and any stale isolated-daily
row, and attest an exact
manifest of every source/config file in the shared runtime bind roots (including
`configs/fotmob/competitions.json`), not only the top-level DAG files.

Planner and completion reads are strict to `fotmob-native-v2`, so every target
is reparsed after deployment. Serving views retain the last successful v1
snapshot only until v2 publishes a successful replacement or an explicit
entity tombstone. A v2 transport/parse failure cannot erase last-good data.

The nine old `bronze.fotmob_*` tables are frozen rollback evidence. They have
no writer after #930, and the legacy scraper/CLI path is removed from the
repository. `bronze.fotmob_transfers` is the sole intentional live read:
`silver.fotmob_transfers` unions its frozen history with the native sliding
feed.

The #930 acceptance boundary is 158 production-cutover scopes. A full historic
catalog crawl is tracked separately in #994.
