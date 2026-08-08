# Task 2 — ESPN native-v2 P1/P2 Silver report

## Scope

Created only the three requested P1/P2 SQL transforms, their focused contract
tests, and this Task 2 report. No legacy ESPN transform, DAG, runner, fixture,
or metadata file was changed.

## RED

Command (from `/root/dpf-1150-espn-silver`):

```bash
/root/.venvs/dpf-test/bin/pytest \
  tests/unit/sql/test_espn_match_events_silver.py \
  tests/unit/sql/test_espn_substitutions_silver.py \
  tests/unit/sql/test_espn_venue_silver.py -q
```

Result before the SQL files existed: `10 failed, 2 passed in 0.60s`.
Nine contract failures were `FileNotFoundError` reads of the three requested SQL
files. Independently, the first DuckDB scalar fixture used a direct `CAST` for
an absent added-time regex capture; it was corrected to `TRY_CAST` so the
fixture matched the required production expression.

## GREEN

Focused command (same command as RED): `12 passed in 0.31s`.

SQL parser command:

```bash
/root/.venvs/dpf-test/bin/python -c "from pathlib import Path; import sqlglot; [sqlglot.parse_one(Path(p).read_text(), read='trino') for p in ['dags/sql/silver/espn_match_events.sql','dags/sql/silver/espn_substitutions.sql','dags/sql/silver/espn_venue.sql']]; print('sqlglot parse: 3/3 passed')"
```

Result: `sqlglot parse: 3/3 passed`.

`git diff --check` result: exit 0, no output.

## Charter baseline delta

Current command:

```bash
/root/.venvs/dpf-test/bin/python scripts/audit_silver_charter.py --check
```

Result: exit 1 only for the pre-existing, unsanctioned Transfermarkt baseline:
`transfermarkt_coach_stints_v2` R3,
`transfermarkt_player_team_season_assignment_v2` R1, and
`transfermarkt_player_xref_global_v2` R3. The detailed Layer-A report recorded
four ERROR findings total because the existing sanctioned
`whoscored_player_season_aggregate` exception also has R1. This is the recorded
clean-`origin/master` baseline described in the task; the Task 2 delta is zero
new ERRORs.

Expected non-blocking new R5 reviews are present exactly for
`espn_substitutions` and `espn_venue`; `espn_match_events` is COMPLIANT.

## Self-review and concerns

- Every physical `*_generation_v2` source name occurs once per SQL file, in a
  `bronze_src_*` CTE. Deduplication precedes JSON expansion and child facts are
  restricted to canonical `played_final` schedule rows.
- Events preserve shootout details but calculate `is_goal` as `scoringPlay AND
  NOT shootout`; `seq` comes from `WITH ORDINALITY`; IDs and card flags use the
  specified detail paths.
- Substitutions intentionally start from incoming rows, so outgoing-only rows
  remain omitted. The outgoing jersey path is the required sibling path.
- Venue performs the required two-stage latest selection: event snapshot first,
  then venue ID. Name is trimmed from the Bronze column and address comes from
  JSON.
- This is static/offline verification only. No live Trino CTAS or production
  row-count validation was run because those are explicitly part of final live
  acceptance, not Task 2.

## Fix round 1

Reviewer feedback found that the initial events SQL stored two backslashes in
the Trino regex literals. In a SQL string literal those are two regex
backslashes, so the expressions would match a literal backslash rather than
the `90'+3'` clock digits. The production literals are now exactly
`'^(\d+)'` and `'\+(\d+)'` (one stored backslash each).

The focused event contracts now bind each output alias to its production AST:
clock input/path, pattern, capture group, event/player JSON paths, goal flags,
and ordinality lineage. Substitution tests similarly bind output fields,
inbound/played predicate, join, and team-qualified dedup key; venue tests bind
the output address/name nodes and each row-number stage. DuckDB remains only a
scalar-behavior fixture, with AST tests preventing divergence from production.

Focused verification after the fix: `14 passed in 0.39s`. A direct live Trino
scalar query was not possible in this checkout because `TRINO_HOST` and
`TRINO_USER` credentials are unavailable; the AST checks assert the exact
single-backslash production literal and the DuckDB fixture confirms
`90'+3'` yields minute `90` and plus-minute `3`.
