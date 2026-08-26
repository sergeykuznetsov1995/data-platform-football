# FBref current-season source-link reconcile

## Purpose

Repair a registry state where the competition index advertises one latest
season but the competition history omits it. The authoritative signal is the
exact `maxseason` label and href in the committed competition-index raw. No
calendar-year heuristic is permitted.

Observed on 2026-08-25:

- competition 6: index advertises `2026` at
  `/en/comps/6/WCQ----UEFA-M-Stats`; history starts at `2022`;
- competition 678: index advertises `2024` at
  `/en/comps/678/UEFA-Euro-qualification-Stats`; history starts at `2021`.

The same false-current pattern affects competitions
`2,3,4,5,6,7,657,664,665,678`. Competition 255 is a non-bug control: its
display label differs, but both index and history carry source season ID
`2026`; it must remain current.

This reconcile is independent of the FBref response body-cap/oversized-page
fix. A genuinely current oversized page such as competition 569 season
2025-2026 still requires the body-cap path after scope is correct.

## Lineage contract

`DISCOVERY_PARSER_VERSION` remains `fbref-discovery-parser-v6`. The change does
not reinterpret generic or typed raw observations and therefore must not
schedule a global typed replay. Registry snapshot UUIDs additionally include
`CURRENT_SEASON_INSTALL_CONTRACT_VERSION =
fbref-current-season-install-source-link-v1`.

A competition-history install snapshot is the deterministic composition of
two immutable inputs:

1. history raw attempt, logical refresh, target, content hash and manifest;
2. successful competition-index snapshot/run plus its exact raw attempt,
   logical refresh, target, content hash and manifest;
3. the index-advertised label, href and source season ID;
4. the explicit install-contract version.

The same history raw plus a changed index snapshot therefore gets a distinct
snapshot identity. Snapshot, season and frontier-provenance metadata all carry
the same two-source dependency. Competition-history discovery relations include
that install snapshot ID, so an unchanged history child can retain immutable
provenance for both index editions without colliding. Reapplying an earlier
authoritative index edition reuses its original deterministic snapshot and
edge identities and reconciles away a season synthesized only by the later
edition.

The installed current season is derived only from:

1. the saved competition-index `maxseason` label and exact href;
2. the latest committed competition-history raw;
3. the source season ID derived from the exact href.

If exactly one of label or href is present, reconciliation fails closed. The
legacy first-history-row fallback is allowed only when both signals are absent.
When both are present, the exact advertised source edition is selected or
synthesized; an old first history row is never substituted.

## Pre-deploy read-only audit

Run in the production control database in a read-only transaction:

```sql
BEGIN READ ONLY;
WITH active AS (
  SELECT c.competition_id, c.name,
         c.metadata ->> 'last_season' AS advertised_label,
         c.metadata ->> 'advertised_current_season_id'
           AS advertised_source_id,
         c.metadata ->> 'last_season_url' AS advertised_url,
         s.season_id, s.label
  FROM fbref_control.competition_registry c
  JOIN fbref_control.season_registry s USING (source, competition_id)
  WHERE c.source = 'fbref'
    AND c.gender = 'male'
    AND c.crawl_state = 'active'
    AND c.lifecycle_state IN ('present', 'missing_once')
    AND c.present
    AND s.lifecycle_state = 'present'
    AND s.present
    AND s.is_current
)
SELECT count(*) AS active_male_competitions,
       count(*) FILTER (WHERE season_id = advertised_source_id)
         AS advertised_source_id_matches,
       count(*) FILTER (WHERE label = advertised_label)
         AS advertised_label_matches,
       count(*) FILTER (
         WHERE season_id IS DISTINCT FROM advertised_source_id
       )
         AS false_current_competitions
FROM active;
COMMIT;
```

Before the new competition-index parse, `advertised_url` and
`advertised_source_id` may be null because the old parser discarded the href.
The immutable index raw remains the source of truth; do not infer either field
from the calendar year.

## Normal repair path

1. Deploy the reviewed code.
2. Run the ordinary current competition-index fetch+parse. This persists the
   exact `last_season_url` into competition-registry metadata.
3. Run the ordinary current competition-history fetch+parse. For histories
   whose newest row is older than the advertised current season, the parser
   installs a synthetic `SeasonRef` from the exact saved href.
4. Let normal frontier reconciliation close the old season's recurring
   targets and open the advertised season root.

No registry rows or raw objects are deleted. The old season remains present
and historical; only `is_current` and downstream frontier scope change.

## Guarded remediation when a fresh parse is not immediate

The remediation script is a reviewed source-only operator adapter. It is not a
deployable runtime file: never copy or install it below `/opt/airflow/scripts`.
The supported mutation remains `FBrefPipeline.remediate_current_seasons` in the
deployed `scrapers` package.

Before using this runbook, execute the complete
`Source-only current-season remediation adapter` immutable-blob block in
`/root/fbref-production-20260825/README.md`, from its `set -Eeuo pipefail`
through its bounded dry-run, in the same shell. Do not excerpt, reimplement, or
rerun only part of that block. It validates the FINAL manifest and clean merged
checkout, resolves and hashes the exact stage-0 Git object, compiles those
bytes, and defines `run_reviewed_remediation`. That function streams
`git cat-file blob "$merged_blob"` directly to the scheduler without creating
a container file. The README block already executes the exact bounded dry-run;
save its JSON output and keep that shell open for the apply calls below. There
is no separate pathname-based `--help`, dry-run, or apply invocation.

The finalizer must exact-ignore only the source-only script recorded in the
FINAL manifest; all deployed files remain subject to its normal audit. The
adapter reads only committed raw and is dry-run by default.

It refuses a competition unless all of the following hold:

- the ID was explicitly listed (maximum 25, no duplicates);
- the competition is active, present, eligible and male;
- registry metadata already contains both maxseason label and href;
- the registry points to a successful competition-index snapshot whose exact
  content hash/raw attempt/logical refresh/target/manifest are available;
- the latest competition-history raw is committed and parses successfully;
- the exact successful history attempt and raw manifest match that raw;
- the resolved current source ID equals the advertised source ID;
- current-season cardinality is singular.

Save the dry-run JSON. It reports both complete inputs: all `history_*` fields
including `history_raw_manifest_key`, and all `index_*` fields including the
successful snapshot/run/content/raw identity.

Apply invocations must be grouped by the control run that owns those exact
history observations. Find it read-only from `fetch_attempt`, then pass it:

```sql
SELECT attempt_id, run_id, logical_refresh_id, target_id,
       content_hash, raw_manifest_key
FROM fbref_control.fetch_attempt
WHERE logical_refresh_id IN ('<dry-run logical refresh IDs>')
  AND status = 'succeeded'
ORDER BY run_id, target_id;
```

For each distinct `run_id`, invoke only its competition IDs:

```bash
run_reviewed_remediation \
  --competition-id 6 --competition-id 678 \
  --apply --source-run-id '<owning control run UUID>'
```

Apply mode enters one supported bounded remediation operation. It holds the
FBref no-writer/publication fence for the entire batch, refuses an active
writer, row-locks and revalidates the exact index snapshot/raw and latest
history frontier/attempt under that fence, then reloads both immutable raw
objects. All snapshot, season, alias and frontier changes use one PostgreSQL
transaction. Any evidence mismatch or failure in any competition rolls the
whole batch back; it never switches to a different raw observation.

## Post-reconcile acceptance

Require all 117 active male competitions to have exactly one current season,
and require the current source ID to match the index-advertised season. A
display label is diagnostic, not source identity: competition 255 is the one
reviewed label alias, and any other label mismatch is a refusal:

```sql
BEGIN READ ONLY;
WITH active AS (
  SELECT c.competition_id,
         c.metadata ->> 'last_season' AS advertised_label,
         c.metadata ->> 'advertised_current_season_id'
           AS advertised_source_id,
         count(*) FILTER (
           WHERE s.present AND s.lifecycle_state = 'present' AND s.is_current
         ) AS current_count,
         max(s.season_id) FILTER (
           WHERE s.present AND s.lifecycle_state = 'present' AND s.is_current
         ) AS current_season_id,
         max(s.label) FILTER (
           WHERE s.present AND s.lifecycle_state = 'present' AND s.is_current
         ) AS current_label
  FROM fbref_control.competition_registry c
  LEFT JOIN fbref_control.season_registry s
    USING (source, competition_id)
  WHERE c.source = 'fbref'
    AND c.gender = 'male'
    AND c.crawl_state = 'active'
    AND c.lifecycle_state IN ('present', 'missing_once')
    AND c.present
  GROUP BY c.competition_id,
           c.metadata ->> 'last_season',
           c.metadata ->> 'advertised_current_season_id'
), classified AS (
  SELECT *,
         current_label IS DISTINCT FROM advertised_label
           AS label_mismatch,
         (competition_id = '255'
           AND current_count = 1
           AND current_season_id = '2026'
           AND advertised_source_id = '2026'
           AND current_label IS NOT NULL
           AND advertised_label IS NOT NULL
           AND current_label IS DISTINCT FROM advertised_label) IS TRUE
           AS approved_label_mismatch,
         current_count <> 1
           OR current_season_id IS DISTINCT FROM advertised_source_id
           AS identity_violation
  FROM active
), summary AS (
  SELECT count(*) AS active_competitions,
         count(*) FILTER (WHERE current_count = 1)
           AS exactly_one_current,
         count(*) FILTER (
           WHERE current_season_id = advertised_source_id
         ) AS advertised_source_id_matches,
         count(*) FILTER (WHERE label_mismatch) AS label_mismatches,
         count(*) FILTER (WHERE approved_label_mismatch)
           AS approved_label_mismatches,
         count(*) FILTER (
           WHERE label_mismatch AND NOT approved_label_mismatch
         ) AS unexpected_label_mismatches,
         count(*) FILTER (
           WHERE identity_violation
              OR (label_mismatch AND NOT approved_label_mismatch)
         ) AS violations
  FROM classified
)
SELECT *,
       active_competitions = 117
         AND exactly_one_current = 117
         AND advertised_source_id_matches = 117
         AND label_mismatches = 1
         AND approved_label_mismatches = 1
         AND unexpected_label_mismatches = 0
         AND violations = 0 AS acceptance_pass
FROM summary;
COMMIT;
```

Expected: `117 / 117 / 117 / 1 / 1 / 0 / 0 / true`.

Before any oversized-page requeue or canary, require the two known stale
playing-time targets to be outside the current proxy/freshness lane. This is a
fail-closed acceptance gate: it must return `2 / 2 / true`.

```sql
BEGIN READ ONLY;
WITH expected(target_id, competition_id, season_id) AS (
  VALUES
    ('fbref:season_stats:6:2022:playingtime', '6', '2022'),
    ('fbref:season_stats:678:2021:playingtime', '678', '2021')
), observed AS (
  SELECT e.target_id,
         NOT s.is_current
           AND s.present
           AND s.lifecycle_state = 'present'
           AND f.refresh_policy = 'daily'
           AND f.state = 'quarantined'
           AND f.next_fetch_at IS NULL
           AND f.last_error_class = 'ScopeQuarantined'
           AND f.last_error_message = 'noncurrent_season' AS accepted
  FROM expected e
  JOIN fbref_control.season_registry s
    ON s.source = 'fbref'
   AND s.competition_id = e.competition_id
   AND s.season_id = e.season_id
  JOIN fbref_control.page_frontier f
    ON f.source = 'fbref'
   AND f.target_id = e.target_id
)
SELECT (SELECT count(*) FROM expected) AS expected_targets,
       count(*) AS observed_targets,
       coalesce(bool_and(accepted), false) AS scope_remediation_accepted
FROM observed;
COMMIT;
```

The operational order is mandatory: complete this current-season remediation
and both acceptance gates first; only then run the independent oversized-page
source-four verification and requeue/canary its genuine-current competition
569 pair. The competition 6/2022 and 678/2021 targets above must not enter that
requeue cohort.

Also verify:

- competition 255 remains current at source season ID `2026`;
- old false-current season roots and recurring descendants are `skipped` or
  historical, never deleted;
- new advertised season roots are current and recurring;
- current-scope freshness no longer includes the two dead
  `playingtime` targets for comp 6/2022 and comp 678/2021;
- no body-cap setting changed as part of this reconcile.
