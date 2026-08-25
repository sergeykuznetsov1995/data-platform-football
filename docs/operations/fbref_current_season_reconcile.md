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
fbref-current-season-install-source-link-v1`. This gives the same immutable raw
and install contract a retry-stable identity, while a changed install contract
gets a distinct registry snapshot identity.

The installed current season is derived only from:

1. the saved competition-index `maxseason` label and exact href;
2. the latest committed competition-history raw;
3. an exact source season ID match, then a display-label match.

If the index advertises a label but neither its href nor a matching history
entry exists, reconciliation fails closed. It never selects the first history
row in that case.

## Pre-deploy read-only audit

Run in the production control database in a read-only transaction:

```sql
BEGIN READ ONLY;
WITH active AS (
  SELECT c.competition_id, c.name,
         c.metadata ->> 'last_season' AS advertised_season,
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
       count(*) FILTER (WHERE season_id = advertised_season)
         AS advertised_source_id_matches,
       count(*) FILTER (WHERE season_id <> advertised_season)
         AS false_current_competitions
FROM active;
COMMIT;
```

Before deployment, `advertised_url` is expected to be null because the old
parser discarded it. The immutable index raw remains the source of truth.

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

The remediation script reads only committed raw and is dry-run by default:

```bash
python scripts/research/remediate_fbref_current_seasons.py \
  --competition-id 2 --competition-id 3 --competition-id 4 \
  --competition-id 5 --competition-id 6 --competition-id 7 \
  --competition-id 657 --competition-id 664 \
  --competition-id 665 --competition-id 678
```

It refuses a competition unless all of the following hold:

- the ID was explicitly listed (maximum 25, no duplicates);
- the competition is active, present, eligible and male;
- registry metadata already contains both maxseason label and href;
- the latest competition-history raw is committed and parses successfully;
- the resolved current source ID equals the advertised source ID;
- current-season cardinality is singular.

Save the dry-run JSON. `history_logical_refresh_id` and
`history_content_hash` are the immutable proof for every proposal.

Apply invocations must be grouped by the control run that owns those exact
history observations. Find it read-only from `fetch_attempt`, then pass it:

```sql
SELECT run_id, logical_refresh_id, target_id, content_hash, raw_manifest_key
FROM fbref_control.fetch_attempt
WHERE logical_refresh_id IN ('<dry-run logical refresh IDs>')
  AND status = 'succeeded'
ORDER BY run_id, target_id;
```

For each distinct `run_id`, invoke only its competition IDs:

```bash
python scripts/research/remediate_fbref_current_seasons.py \
  --competition-id 6 --competition-id 678 \
  --apply --source-run-id '<owning control run UUID>'
```

Before any write, apply mode verifies exactly one successful attempt in that
run has the target ID, logical refresh ID, content hash, and raw manifest key
reported by dry-run. It then calls the same competition parse/reconcile path
as a normal wave. A mismatch aborts without using another raw observation.

## Post-reconcile acceptance

Require all 117 active male competitions to have exactly one current season,
and require the current source ID to match the index-advertised season:

```sql
BEGIN READ ONLY;
WITH active AS (
  SELECT c.competition_id,
         c.metadata ->> 'last_season' AS advertised_season,
         count(*) FILTER (
           WHERE s.present AND s.lifecycle_state = 'present' AND s.is_current
         ) AS current_count,
         max(s.season_id) FILTER (
           WHERE s.present AND s.lifecycle_state = 'present' AND s.is_current
         ) AS current_season_id
  FROM fbref_control.competition_registry c
  LEFT JOIN fbref_control.season_registry s
    USING (source, competition_id)
  WHERE c.source = 'fbref'
    AND c.gender = 'male'
    AND c.crawl_state = 'active'
    AND c.lifecycle_state IN ('present', 'missing_once')
    AND c.present
  GROUP BY c.competition_id, c.metadata ->> 'last_season'
)
SELECT count(*) AS active_competitions,
       count(*) FILTER (WHERE current_count = 1) AS exactly_one_current,
       count(*) FILTER (
         WHERE current_season_id = advertised_season
       ) AS advertised_source_id_matches,
       count(*) FILTER (
         WHERE current_count <> 1
            OR current_season_id <> advertised_season
       ) AS violations
FROM active;
COMMIT;
```

Expected: `117 / 117 / 117 / 0`.

Also verify:

- competition 255 remains current at source season ID `2026`;
- old false-current season roots and recurring descendants are `skipped` or
  historical, never deleted;
- new advertised season roots are current and recurring;
- current-scope freshness no longer includes the two dead
  `playingtime` targets for comp 6/2022 and comp 678/2021;
- no body-cap setting changed as part of this reconcile.
