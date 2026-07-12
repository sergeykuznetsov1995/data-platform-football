-- =============================================================================
-- Silver: whoscored_events_spadl
-- =============================================================================
-- Normalises `iceberg.bronze.whoscored_events_current` (latest successful
-- manifest batch per match; Opta-derived JSON, 39 distinct
-- WhoScored type values, ~700K rows / season for APL) into a SPADL-shaped
-- canonical action vocabulary plus a single proprietary supplement
-- (`ball_recovery`).
--
-- Spec reference: docs/research/R3_spadl_coverage.md (D2 mapping table).
--   * Coverage measured 89.97% (high+medium) on 2425+2526 corpus.
--   * Decision (R3): SPADL primary + open-action proprietary supplement.
--   * Mapping logic location: this file (own SQL, NOT socceraction; D4
--     deferred — adapter cost > value at 89.97% coverage).
--
-- DAG-integration note: silver_tasks.run_silver_transform() wraps this SELECT
-- in `CREATE TABLE iceberg.silver.whoscored_events_spadl AS ...`. This file
-- MUST stay a pure SELECT (no CREATE TABLE, no DDL).
--
-- =============================================================================
-- Output schema (frozen for E3 wave-1)
-- =============================================================================
--   event_id              varchar     source-backed stable id for v2 rows;
--                                     legacy fallback = game_id || '_' || seq
--   source_event_id_raw   varchar     global Opta `id` from matchCentreData
--   team_event_id_raw     varchar     team-local Opta `eventId` sequence
--   related_team_event_id_raw varchar  raw team-local `relatedEventId`; this is
--                                     not a FK to source_event_id_raw
--   match_id              varchar     CAST(game_id AS varchar)
--   team_id_raw           varchar     bronze team_id (NOT resolved via xref —
--                                     that is a Gold-job concern)
--   team_name_raw         varchar     bronze team name passthrough (#736: Gold
--                                     timeline resolves xref_team by name)
--   player_id_raw         varchar     bronze player_id (double → varchar)
--   related_player_id_raw varchar     bronze related_player_id (double → varchar;
--                                     #736: assist / sub-on actor for Gold timeline)
--   period                varchar     period passthrough
--   expanded_minute       integer     TRY_CAST from bigint
--   minute                bigint      bronze minute passthrough (#736: 0-based
--                                     cumulative within half — Gold timeline base)
--   second                integer     bronze second (double → integer) (#736)
--   x, y, end_x, end_y    double      pitch coordinates (passthrough)
--   action_canonical      varchar     ENUM (25 values, see below)
--   action_source         varchar     literal 'whoscored_spadl_proprietary_v1'
--   action_version        varchar     literal 'v1'
--   _action_source_note   varchar     original WhoScored type (audit trail)
--                                     plus paired-flag for Aerial duels
--                                     ('aerial_paired' suffix)
--   _action_confidence    varchar     'high' | 'medium' | 'low' | 'unmappable'
--   outcome_success       boolean     outcome_type = 'Successful'
--   qualifiers_raw        varchar     preserved JSON-string (audit / Gold split)
--   _bronze_ingested_at   timestamp   from bronze _ingested_at
--   league                varchar     partition key passthrough
--   season                varchar     partition key passthrough
--
-- Primary key:  (match_id, event_id)
--
-- =============================================================================
-- action_canonical enum — 25 values (22 SPADL + 2 proprietary + 'unknown')
-- =============================================================================
--   SPADL canonical (22):
--     pass, cross, throw_in, freekick_crossed, freekick_short,
--     corner_crossed, corner_short, take_on, foul, tackle, interception,
--     shot, shot_penalty, shot_freekick, keeper_save, keeper_claim,
--     keeper_punch, keeper_pick_up, clearance, bad_touch, dribble,
--     goalkick
--   Proprietary supplement (2):
--     ball_recovery       (R3.D2 #2; SPADL collapses recovery into
--                          interception — 5.33% objem too high to lose;
--                          confidence='low' so downstream ML can filter)
--     own_goal            (#572; Goal + OwnGoal qualifier. Kept distinct from
--                          the shot family so the deflecting defender is not
--                          credited a shot; confidence='medium' via type='Goal')
--   Sentinel (1):
--     unknown             (meta-events: Card / Substitution / Goal / Start
--                          / End / FormationSet / FormationChange /
--                          OffsideProvoked / OffsideGiven / CornerAwarded;
--                          confidence='unmappable')
--
-- DQ invariant (E3.8): every row's action_canonical MUST be in the 25-value
-- enum above. Any other value -> hard fail.
--
-- =============================================================================
-- R3.D2 mapping table — embedded reference (39 WhoScored types)
-- =============================================================================
--   #  WhoScored type    SPADL action(s)                         confidence
--   -- ----------------  --------------------------------------  ----------
--    1 Pass              pass / cross / corner_crossed /         medium
--                        corner_short / freekick_crossed /
--                        freekick_short / throw_in / goalkick
--                        (qualifier-driven; see CASE tree)
--    2 BallRecovery      ball_recovery (proprietary)             low
--    3 BallTouch         bad_touch (Unsuccessful);               medium
--                        Successful preserved as bad_touch with
--                        outcome_success=true (consumer filters
--                        if needed — keeps row-count parity)
--    4 Aerial            tackle (both rows kept;                 medium
--                        _action_source_note='aerial_paired';
--                        dedup is consumer's call)
--    5 Clearance         clearance                               high
--    6 Foul              foul                                    high
--    7 TakeOn            take_on                                 high
--    8 Tackle            tackle                                  high
--    9 CornerAwarded     unknown (marker)                        unmappable
--   10 Dispossessed      bad_touch                               medium
--   11 Interception      interception                            high
--   12 BlockedPass       interception                            high
--   13 Challenge         tackle (failed)                         high
--   14 SavedShot         shot / shot_freekick / shot_penalty     medium
--   15 Save              keeper_save                             high
--   16 KeeperPickup      keeper_pick_up                          high
--   17 MissedShots       shot / shot_freekick / shot_penalty     medium
--   18 SubstitutionOff   unknown (meta)                          unmappable
--   19 SubstitutionOn    unknown (meta)                          unmappable
--   20 End               unknown (meta)                          unmappable
--   21 Card              unknown (meta)                          unmappable
--   22 Start             unknown (meta)                          unmappable
--   23 OffsideProvoked   unknown (marker)                        unmappable
--   24 OffsideGiven      unknown (marker)                        unmappable
--   25 OffsidePass       pass                                    low
--   26 FormationChange   unknown (meta)                          unmappable
--   27 Goal              shot / shot_freekick / shot_penalty     medium
--                        (Opta type-16 = standalone scored shot, #462);
--                        Goal + OwnGoal qualifier -> own_goal (#572)
--   28 FormationSet      unknown (meta)                          unmappable
--   29 Claim             keeper_claim                            high
--   30 Error             bad_touch (degraded)                    low
--   31 ShieldBallOpp     dribble (degraded)                      low
--   32 KeeperSweeper     keeper_save                             medium
--   33 Punch             keeper_punch                            high
--   34 ShotOnPost        shot / shot_freekick / shot_penalty     medium
--   35 Smother           keeper_save                             high
--   36 PenaltyFaced      keeper_save                             low
--   37 GoodSkill         dribble                                 medium
--   38 ChanceMissed      shot                                    low
--   39 CrossNotClaimed   keeper_claim                            medium
--
--   Sanity: 39 / 39 distinct WhoScored types from D1 audit covered.
--   Sum of events by row = 695,148 (matches D1 total).
--
-- =============================================================================
-- Pass routing (the dominant 62.89% bucket — drives overall coverage)
-- =============================================================================
--   `qualifiers` is a JSON-string array of NESTED objects
--   ({"type": {"value": N, "displayName": "X"}}); checked via regexp_like on
--   the `displayName` field (NOT the flat `type` — that holds a nested object):
--     `"displayName":"ThrowIn"`        -> throw_in
--     `"displayName":"GoalKick"`       -> goalkick
--     `"displayName":"CornerTaken"` + `"displayName":"Cross"`   -> corner_crossed
--     `"displayName":"CornerTaken"` (without Cross)             -> corner_short
--     `"displayName":"FreekickTaken"` + `"displayName":"Cross"` -> freekick_crossed
--     `"displayName":"FreekickTaken"` (without Cross)           -> freekick_short
--     `"displayName":"Cross"` (none of the above)              -> cross
--     no qualifier OR qualifiers=NULL                   -> pass (medium-conf
--                                                         fallback; 7.05%
--                                                         no-qualifier rows
--                                                         per R3.D1)
--
--   Note: for the 7.05% no-qualifier rows we DELIBERATELY collapse to `pass`
--   (NOT `unknown`). Marking 7% of the dominant 62% bucket as unknown would
--   destroy the SPADL signal; the explicit medium-confidence label lets
--   downstream consumers filter if needed.
-- =============================================================================
--
-- Shot-subtype routing (SavedShot / MissedShots / ShotOnPost / ChanceMissed):
--     `"displayName":"Penalty"`        -> shot_penalty
--     `"displayName":"DirectFreekick"` -> shot_freekick
--     default                          -> shot
--
-- =============================================================================
-- Edge cases / known fidelity losses (deferred to Gold or follow-ups)
-- =============================================================================
--   * Aerial paired duels: keep BOTH rows (winner + loser) with
--     action_canonical='tackle' and _action_source_note='aerial_paired:<orig>'.
--     SPADL convention writes one row; we keep both for row-count parity
--     with bronze. Deduplication strategy is a Gold/E4 decision.
--   * BallTouch+Successful: kept as bad_touch with outcome_success=true.
--     R3.D2 originally proposed dropping these (precursor of next action),
--     but to preserve parity we route the same as Unsuccessful and rely on
--     consumer filters (this also avoids a 17K-row Bronze-Silver gap).
--   * Foul+Unsuccessful (foul-suffered marker): kept as foul with
--     outcome_success=false. Gold can split if needed.
--   * Goal events (~1,288 rows): mapped to the shot family (shot /
--     shot_penalty / shot_freekick by qualifier). Opta type-16 Goal IS the
--     standalone scored-shot record — verified 0/1288 goals have a paired
--     SavedShot/MissedShots in the same second (#462). Counting Goal as a
--     shot matches whoscored_player_match_aggregate (which counts it from
--     bronze) and lifts WhoScored shots to a realistic ~25.2/match.
--     EXCEPTION (#572): a Goal carrying the `OwnGoal` qualifier (~49 rows)
--     routes to action_canonical='own_goal' instead — own-goals must not be
--     credited as a shot to the deflecting defender.
--   * CornerAwarded / OffsideProvoked / OffsideGiven: marker events with
--     no SPADL action; mapped to 'unknown' to preserve event-count parity.
--   * Bronze current view exposes only successful logical commits. The V2
--     migration already collapsed legacy re-scrape duplicates on the complete
--     event natural key before seeding one successful manifest batch per game;
--     new V2 commits require source_event_id = raw Opta `id`, reject duplicate
--     global IDs and retain the distinct team-local `eventId` separately.
--     Silver therefore preserves that strict-current row set one-for-one.
--     Repeating the dedup here used a 20-column ROW_NUMBER partition key
--     (including qualifiers JSON) and exhausted Trino's 8.3 GB heap at 28M
--     rows while re-enforcing an upstream invariant.
--   * Cross-source league/season/team-id resolution: NOT done here.
--     Silver is a pure normaliser; xref join lives in Gold (E3.3 fct_event).
--
-- =============================================================================
-- Verification status
-- =============================================================================
--   * Legacy Bronze schema verified via DESCRIBE on 2026-07-12; V2 adds the
--     explicit global/team-local event identity fields described above.
--   * 39 distinct `type` values verified; all 39 covered in CASE tree.
--   * v2 event_id = 'ws:' || game_id || ':' || source_event_id. Historical
--     rows with source_event_id=NULL retain the established game-local
--     chronological sequence. This is the only remaining window: it partitions
--     by (league, season, game_id), orders on seven narrow scalar columns, then
--     breaks exact chronology ties with a 32-byte SHA-256 of the complete
--     migration natural key. The hash excludes _ingested_at, so replay order
--     and capture time cannot renumber legacy ids. It is an ORDER BY value only:
--     the established game_id_00001 formula/format remains unchanged.
--   * Native Trino EXPLAIN verified on 2026-07-11 with an outer
--     (league, season) scope: the predicate reaches the Iceberg ScanFilter and
--     the remaining window stays partitioned by one match.
-- =============================================================================

WITH seq AS (
    SELECT
        game_id,
        source_event_id,
        team_event_id,
        period,
        minute,
        second,
        expanded_minute,
        type,
        outcome_type,
        team_id,
        player_id,
        x,
        y,
        end_x,
        end_y,
        qualifiers,
        related_team_event_id,
        related_player_id,
        team,
        league,
        season,
        _ingested_at,
        ROW_NUMBER() OVER (
            -- league/season lead the partition key so the E3 partition runner's
            -- outer scope predicate can push below this window. game_id keeps
            -- each partition bounded to one match (~1.5K rows), unlike the
            -- removed 20-column corpus-wide dedup key containing qualifiers.
            PARTITION BY league, season, game_id
            ORDER BY
                CASE period
                    WHEN 'PreMatch'                THEN 0
                    WHEN 'FirstHalf'               THEN 1
                    WHEN 'SecondHalf'              THEN 2
                    WHEN 'FirstPeriodOfExtraTime'  THEN 3
                    WHEN 'SecondPeriodOfExtraTime' THEN 4
                    WHEN 'PenaltyShootout'         THEN 5
                    WHEN 'PostGame'                THEN 6
                    ELSE 99
                END,
                minute, second, expanded_minute, type, x, y,
                -- The chronological fields above are not unique: two events
                -- can share the same clock, type and start coordinates. Hash
                -- the complete migration natural key as one fixed-width final
                -- key instead of retaining its wide strings/qualifiers as
                -- separate sort channels. _ingested_at is deliberately absent.
                sha256(
                    to_utf8(
                        json_format(
                            CAST(
                                ROW(
                                    league, season, game_id, source_event_id,
                                    period, minute, second, expanded_minute,
                                    type, outcome_type, team_id, player_id,
                                    x, y, end_x, end_y, qualifiers,
                                    related_team_event_id, related_player_id, team
                                ) AS JSON
                            )
                        )
                    )
                )
        ) AS event_seq
    FROM iceberg.bronze.whoscored_events_current
)

SELECT
    -- ========= Source-backed stable PK with legacy fallback =========
    CASE
        WHEN source_event_id IS NOT NULL THEN
            'ws:' || CAST(game_id AS varchar)
                  || ':' || CAST(source_event_id AS varchar)
        ELSE CAST(game_id AS varchar)
                  || '_' || LPAD(CAST(event_seq AS varchar), 5, '0')
    END                                                      AS event_id,
    CAST(game_id AS varchar)                                 AS match_id,

    -- Keep the source relationship explicit instead of forcing consumers to
    -- parse the canonical event_id string.
    CAST(source_event_id AS varchar)                          AS source_event_id_raw,
    CAST(team_event_id AS varchar)                            AS team_event_id_raw,
    CAST(related_team_event_id AS varchar)                    AS related_team_event_id_raw,

    -- ========= Raw entity references (resolved in Gold via xref) =========
    -- bronze stores team_id / player_id as DOUBLE; direct CAST to varchar
    -- yields scientific notation ('9.5408E4'), which breaks string equality
    -- against xref_*.source_id (canonical integer-string '95408'). Round
    -- through BIGINT first to keep digits intact. NULL stays NULL.
    CAST(CAST(team_id   AS BIGINT) AS varchar)               AS team_id_raw,
    -- #736: raw team NAME passthrough — Gold timeline resolves xref_team
    -- (source='whoscored') by name, and flips own-goal credit by name.
    team                                                     AS team_name_raw,
    CAST(CAST(player_id AS BIGINT) AS varchar)               AS player_id_raw,
    -- #736: assist / substitution-on actor — same double→varchar cast as
    -- player_id_raw so Gold timeline joins xref_player directly.
    CAST(CAST(related_player_id AS BIGINT) AS varchar)       AS related_player_id_raw,

    -- ========= Time / pitch coordinates =========
    CAST(period AS varchar)                                  AS period,
    TRY_CAST(expanded_minute AS integer)                     AS expanded_minute,
    -- #736: raw Opta minute (0-based cumulative within half) + second — Gold
    -- timeline derives FBref-scale minute/minute_added/event_seq from these.
    minute                                                   AS minute,
    CAST(second AS integer)                                  AS second,
    x,
    y,
    end_x,
    end_y,

    -- ========= SPADL canonical action (25-value enum) =========
    CASE
        -- ---------- Pass routing (qualifier-driven) ----------
        WHEN type = 'Pass' THEN
            CASE
                WHEN qualifiers IS NULL OR qualifiers = '' OR qualifiers = '[]'
                    THEN 'pass'
                WHEN regexp_like(qualifiers, '"displayName"\s*:\s*"ThrowIn"')
                    THEN 'throw_in'
                WHEN regexp_like(qualifiers, '"displayName"\s*:\s*"GoalKick"')
                    THEN 'goalkick'
                WHEN regexp_like(qualifiers, '"displayName"\s*:\s*"CornerTaken"')
                     AND regexp_like(qualifiers, '"displayName"\s*:\s*"Cross"')
                    THEN 'corner_crossed'
                WHEN regexp_like(qualifiers, '"displayName"\s*:\s*"CornerTaken"')
                    THEN 'corner_short'
                WHEN regexp_like(qualifiers, '"displayName"\s*:\s*"FreekickTaken"')
                     AND regexp_like(qualifiers, '"displayName"\s*:\s*"Cross"')
                    THEN 'freekick_crossed'
                WHEN regexp_like(qualifiers, '"displayName"\s*:\s*"FreekickTaken"')
                    THEN 'freekick_short'
                WHEN regexp_like(qualifiers, '"displayName"\s*:\s*"Cross"')
                    THEN 'cross'
                ELSE 'pass'
            END

        -- ---------- OffsidePass — flagged offside pass ----------
        WHEN type = 'OffsidePass' THEN 'pass'

        -- ---------- Direct SPADL matches ----------
        WHEN type = 'Foul'         THEN 'foul'
        WHEN type = 'TakeOn'       THEN 'take_on'
        WHEN type = 'Tackle'       THEN 'tackle'
        WHEN type = 'Interception' THEN 'interception'
        WHEN type = 'BlockedPass'  THEN 'interception'
        WHEN type = 'Challenge'    THEN 'tackle'
        WHEN type = 'Clearance'    THEN 'clearance'

        -- ---------- Aerial — paired duels (both rows kept as 'tackle') ----------
        WHEN type = 'Aerial'       THEN 'tackle'

        -- ---------- BallTouch / Dispossessed / Error / ShieldBallOpp ----------
        WHEN type = 'BallTouch'    THEN 'bad_touch'
        WHEN type = 'Dispossessed' THEN 'bad_touch'
        WHEN type = 'Error'        THEN 'bad_touch'
        WHEN type = 'ShieldBallOpp' THEN 'dribble'
        WHEN type = 'GoodSkill'    THEN 'dribble'

        -- ---------- BallRecovery — proprietary supplement (NOT SPADL) ----------
        WHEN type = 'BallRecovery' THEN 'ball_recovery'

        -- ---------- Own-goal (Goal + OwnGoal qualifier) ----------
        --   Own-goals arrive as type='Goal' carrying the `OwnGoal` qualifier
        --   (NOT a distinct type='OwnGoal' row). Route to a dedicated action so
        --   the deflecting defender is NOT counted a shot by shot-family
        --   consumers (#572). MUST precede the shot block below — a plain Goal
        --   (no OwnGoal qualifier) falls through to the shot routing. Team-credit
        --   for the goal lives in Gold fct_match_timeline, not here.
        WHEN type = 'Goal'
             AND regexp_like(COALESCE(qualifiers, ''), '"displayName"\s*:\s*"OwnGoal"')
            THEN 'own_goal'

        -- ---------- Shot variants (qualifier-driven sub-routing) ----------
        --   `Goal` (Opta type 16) is the standalone scored-shot record, NOT a
        --   redundant marker: 0/1288 goals have a paired SavedShot/MissedShots
        --   in the same second (#462). It joins the shot family so the most
        --   valuable ~11% of shots are not lost from action_canonical='shot'.
        WHEN type IN ('SavedShot', 'MissedShots', 'ShotOnPost', 'ChanceMissed', 'Goal') THEN
            CASE
                WHEN regexp_like(COALESCE(qualifiers, ''), '"displayName"\s*:\s*"Penalty"')
                    THEN 'shot_penalty'
                -- Shot taken directly from a free kick carries the
                -- `DirectFreekick` qualifier (NOT `FreekickTaken`, which only
                -- tags the *pass* set-piece — it never appears on a shot event).
                WHEN regexp_like(COALESCE(qualifiers, ''), '"displayName"\s*:\s*"DirectFreekick"')
                    THEN 'shot_freekick'
                ELSE 'shot'
            END

        -- ---------- Goalkeeper actions ----------
        WHEN type = 'Save'           THEN 'keeper_save'
        WHEN type = 'KeeperSweeper'  THEN 'keeper_save'
        WHEN type = 'Smother'        THEN 'keeper_save'
        WHEN type = 'PenaltyFaced'   THEN 'keeper_save'
        WHEN type = 'KeeperPickup'   THEN 'keeper_pick_up'
        WHEN type = 'Punch'          THEN 'keeper_punch'
        WHEN type = 'Claim'          THEN 'keeper_claim'
        WHEN type = 'CrossNotClaimed' THEN 'keeper_claim'

        -- ---------- Meta / marker events -> 'unknown' ----------
        --   Card, SubstitutionOn, SubstitutionOff, Start, End,
        --   FormationSet, FormationChange, CornerAwarded,
        --   OffsideProvoked, OffsideGiven (any other unforeseen type)
        ELSE 'unknown'
    END                                                      AS action_canonical,

    CAST('whoscored_spadl_proprietary_v1' AS varchar)        AS action_source,
    CAST('v1'                              AS varchar)        AS action_version,

    -- ========= Audit trail =========
    --   Aerial rows get a paired-flag suffix so downstream dedup is trivial.
    CASE
        WHEN type = 'Aerial' THEN 'aerial_paired:Aerial'
        ELSE type
    END                                                      AS _action_source_note,

    -- ========= Confidence per R3.D2 ==========
    CASE
        -- high
        WHEN type IN (
            'Foul', 'TakeOn', 'Tackle', 'Interception', 'BlockedPass',
            'Challenge', 'Clearance', 'Save', 'KeeperPickup', 'Claim',
            'Punch', 'Smother'
        ) THEN 'high'

        -- medium (qualifier-dependent OR direct-but-paired/degraded)
        WHEN type IN (
            'Pass', 'Aerial', 'BallTouch', 'Dispossessed',
            'SavedShot', 'MissedShots', 'ShotOnPost', 'Goal',
            'KeeperSweeper', 'GoodSkill', 'CrossNotClaimed'
        ) THEN 'medium'

        -- low
        WHEN type IN (
            'BallRecovery', 'OffsidePass', 'Error', 'ShieldBallOpp',
            'PenaltyFaced', 'ChanceMissed'
        ) THEN 'low'

        -- unmappable (meta + markers)
        ELSE 'unmappable'
    END                                                      AS _action_confidence,

    -- ========= Outcome flag =========
    (outcome_type = 'Successful')                            AS outcome_success,

    -- ========= Preserved JSON-string for Gold splits / audit =========
    qualifiers                                               AS qualifiers_raw,

    -- ========= Lineage =========
    _ingested_at                                             AS _bronze_ingested_at,

    -- ========= Partition keys (passthrough) =========
    league,
    season

FROM seq
