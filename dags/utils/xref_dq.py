"""
DQ checks for Silver xref-tables (E1).
======================================

This module is the T6 deliverable of the Medallion E1 redesign. It is
consumed by ``dag_transform_xref.validate_xref`` and is intentionally
**read-only** with respect to Iceberg — every query is a SELECT.

Responsibility
--------------
**Per-table DQ checks** (PK uniqueness, NULL guards, enum compliance,
row-count, coverage). Built on the existing universal
:mod:`utils.data_quality` primitives — we do NOT modify
``data_quality.py``. Enum / coverage are implemented as targeted
``CHECK.row_count`` invocations with a ``WHERE`` predicate that
selects offending rows; offender count > 0 fails the check.

Confidence allow-lists (SOURCE OF TRUTH for enum compliance)
------------------------------------------------------------
Verified against the SQL files on 2026-05-08:

* ``xref_team``     — {``name_alias``, ``orphan``}
* ``xref_match``    — {``exact``}
* ``xref_referee``  — {``name_normalize``}
* ``xref_player``   — {``exact``, ``name_team``, ``name_team_jersey``,
                       ``name_team_dob``, ``orphan``}  (``name_team_dob``
                       is live since the DOB-corroboration tier; jersey
                       remains a reserved STUB.)

Sources (xref_team / xref_referee / xref_match / xref_player) values
are also enforced via enum checks against the documented schema.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from utils.data_quality import CHECK, Check, _get_conn, _qualify  # type: ignore[attr-defined]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Enum / coverage helpers (built on row_count + WHERE — NO data_quality.py edits)
# ---------------------------------------------------------------------------

def check_enum_compliance(
    table: str,
    column: str,
    allowed: List[str],
    severity: str = 'ERROR',
    name: Optional[str] = None,
) -> Check:
    """Return a Check that fails if ``column`` has any value outside ``allowed``.

    Implemented as ``row_count(min=0, max=0, WHERE col NOT IN (...))``.
    A NULL in ``column`` is **not** an enum violation by definition (we
    have dedicated ``no_nulls`` checks for required columns); the WHERE
    predicate uses ``column NOT IN (...)`` which already filters NULL.
    """
    if not allowed:
        raise ValueError("check_enum_compliance requires a non-empty allowed list")
    # Quote each value as a SQL string literal; reject embedded quote
    # characters early to keep the predicate safe for f-string interpolation.
    safe_vals: List[str] = []
    for v in allowed:
        if not isinstance(v, str) or "'" in v or ';' in v or '--' in v:
            raise ValueError(f"Unsafe enum value: {v!r}")
        safe_vals.append(f"'{v}'")
    allowed_csv = ", ".join(safe_vals)
    short = table.split('.')[-1]
    return CHECK.row_count(
        table=table,
        min_rows=0,
        max_rows=0,
        where=f"{column} NOT IN ({allowed_csv})",
        severity=severity,
        name=name or f"enum_compliance[{short}.{column}]",
    )


def _in_scope_leagues() -> int:
    """Number of competitions with ``in_scope: true`` (E8b: 5, was 1)."""
    from utils.medallion_config import get_in_scope_competitions
    return max(1, len(get_in_scope_competitions()))


def _spine_season_predicate() -> str:
    """SQL predicate limiting a check to seasons the FBref spine covers.

    Bronze depth is NOT uniform: ``matchhistory`` carries 26 seasons and
    ``espn`` 13, while the FBref spine only goes back to 1617. A bridged-row
    coverage ratio computed over the WHOLE table therefore measures scrape
    depth, not bridge quality — every pre-spine row is an unavoidable orphan
    (``bridge_coverage[xref_match.matchhistory]`` sat at 7.5% for that reason
    alone). Scoping to the in-scope season set makes the ratio mean what its
    name says. Seasons come from competitions.yaml, so onboarding a season
    widens the window automatically.
    """
    from utils.medallion_config import (
        get_competition_seasons,
        get_in_scope_competitions,
    )
    slugs = sorted({
        f"{int(s):04d}"
        for league in get_in_scope_competitions()
        for s in get_competition_seasons(league)
    })
    if not slugs:
        return 'TRUE'
    return "season IN (" + ", ".join(f"'{s}'" for s in slugs) + ")"


# ---------------------------------------------------------------------------
# Per-table DQ definitions
# ---------------------------------------------------------------------------

def build_xref_team_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_team``.

    Orphan-rate (coverage-style) is **not** in this list — ratios cannot
    be expressed via ``row_count``. Instead the DAG callable runs
    :func:`evaluate_orphan_rate_per_source` after the standard checks
    and appends a synthetic CheckResult to the report.

    Row count is bounded above as well — a runaway UNION of duplicate
    rows would silently inflate the table; an upper bound flags it.
    """
    table = 'iceberg.silver.xref_team'
    return [
        # Row count: 8 sources × ~50 distinct teams across seasons — min 400.
        # Upper bound 5000 per in-scope league covers 5 seasons of growth
        # before triggering (E8b: ×5 leagues).
        CHECK.row_count(table, min_rows=400, max_rows=5000 * _in_scope_leagues()),

        # PK uniqueness — guaranteed by SQL GROUP BY but DQ-enforced.
        CHECK.no_duplicates(
            table,
            pk=['source', 'source_id', 'league', 'season'],
        ),

        # Required columns must be non-NULL.
        CHECK.no_nulls(table, cols=['canonical_id', 'source', 'source_id']),

        # Enum compliance — confidence ∈ {'name_alias', 'orphan'}
        check_enum_compliance(
            table, 'confidence',
            allowed=['name_alias', 'orphan'],
            severity='ERROR',
        ),

        # Source enum (11 sources documented in xref_team.sql.j2;
        # transfermarkt + capology added in issue #192 for team-finance facts;
        # sofifa added in #601 for game-side team ratings)
        check_enum_compliance(
            table, 'source',
            allowed=['fbref', 'understat', 'whoscored', 'sofascore',
                     'fotmob', 'matchhistory', 'clubelo', 'espn',
                     'transfermarkt', 'capology', 'sofifa'],
            severity='ERROR',
        ),
    ]


def build_xref_match_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_match`` (Phase B — 7-source cascade).

    Source enum covers FBref spine + 6 cascaded sources. PK is composite
    ``(canonical_id, source)`` because the same FBref canonical legitimately
    appears under multiple ``source`` values (the bridged rows). Per-source
    bridge coverage is enforced via ``CHECK.coverage`` with the universal
    two-tier semantics: ``ratio = COUNT_IF(confidence != 'orphan') / COUNT(*)``.
    """
    table = 'iceberg.silver.xref_match'
    sources = [
        'fbref', 'whoscored', 'understat', 'sofascore',
        'fotmob', 'matchhistory', 'espn',
    ]
    checks: List[Check] = [
        # 5 seasons × ~380 fixtures × 7 sources ≈ 13K per league; cap 60K each,
        # with headroom (E8b: ×5 leagues). NB: matchhistory/espn also carry
        # pre-spine seasons, which is why the cap is generous.
        CHECK.row_count(table, min_rows=1900, max_rows=60_000 * _in_scope_leagues()),

        # PK is composite — bridged sources share canonical_id with FBref.
        CHECK.no_duplicates(table, pk=['canonical_id', 'source']),

        CHECK.no_nulls(table, cols=['canonical_id', 'source', 'source_id']),

        check_enum_compliance(
            table, 'source',
            allowed=sources,
            severity='ERROR',
        ),

        # 'exact' = FBref spine, 'date_team_match' = bridged cascade row,
        # 'orphan' = source row with no FBref counterpart.
        check_enum_compliance(
            table, 'confidence',
            allowed=['exact', 'date_team_match', 'orphan'],
            severity='ERROR',
        ),
    ]

    # Per-source bridge coverage (skip fbref — it's the spine, always 'exact').
    # Two-tier semantics via the data_quality coverage runner:
    #   ratio >= 0.95 -> OK; 0.80-0.95 -> WARNING; <0.80 -> ERROR.
    # Scoped to spine seasons — see _spine_season_predicate().
    season_pred = _spine_season_predicate()
    for src in sources:
        if src == 'fbref':
            continue
        checks.append(CHECK.coverage(
            table=table,
            condition="confidence != 'orphan'",
            where=f"source = '{src}' AND {season_pred}",
            warn_threshold=0.95,
            error_threshold=0.80,
            severity='WARNING',  # runner promotes to ERROR when ratio < 0.80
            name=f'bridge_coverage[xref_match.{src}]',
        ))

    return checks


# Referee known-pair regression anchors (issue #143). Active APL referees that
# MUST resolve to one canonical_id carrying BOTH fbref + matchhistory rows —
# the curated-config analogue of xref_player's KNOWN_PAIRS guard. If
# referee_aliases.yaml drops/breaks one of these, the DQ check below ERRORs.
KNOWN_REFEREE_CANONICALS = (
    'ref_michael_oliver',
    'ref_anthony_taylor',
    'ref_paul_tierney',
    'ref_craig_pawson',
)


def build_xref_referee_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_referee`` (issue #143 — curated config).

    Confidence allow-list mirrors the xref_team contract (``name_alias`` /
    ``orphan``) now that referee identity comes from referee_aliases.yaml.
    Adds the canonical_id-format guard, the per-canonical dup guard (issue #70
    fan-out class), and a known-referee regression guard.
    """
    table = 'iceberg.silver.xref_referee'
    known_csv = ', '.join(f"'{c}'" for c in KNOWN_REFEREE_CANONICALS)
    return [
        CHECK.row_count(table, min_rows=200, max_rows=5000),

        CHECK.no_duplicates(
            table,
            pk=['source', 'source_id', 'league', 'season'],
        ),

        CHECK.no_nulls(table, cols=['canonical_id', 'source', 'source_id']),

        check_enum_compliance(
            table, 'source',
            allowed=['fbref', 'matchhistory', 'fotmob'],
            severity='ERROR',
        ),

        # confidence — aligned with xref_team: 'name_alias' (matched in
        # referee_aliases.yaml) | 'orphan' (no alias row). Old 'name_normalize'
        # is gone with the pure-SQL slug path.
        check_enum_compliance(
            table, 'confidence',
            allowed=['name_alias', 'orphan'],
            severity='ERROR',
        ),

        # canonical_id format guard — 'ref_' (aliased) or
        # 'fb_ref_'/'mh_ref_'/'fm_ref_' (orphan fallback). Mirrors xref_player's
        # prefix guard. 'fm_ref_' = FotMob orphan (issue #270).
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=0,
            where="NOT regexp_like(canonical_id, '^(ref|fb_ref|mh_ref|fm_ref)_.+$')",
            severity='ERROR',
            name='canonical_id_format[xref_referee]',
        ),

        # NB: unlike xref_player, we intentionally do NOT add a
        # no_duplicates_per_canonical_season guard. Referees are name-keyed
        # (no source ID), so multiple raw spellings legitimately collapse to one
        # canonical within a (source, season) — e.g. MatchHistory 'J Gillett'
        # and 'J Gillett ' (trailing space), or 'M Oliver' + the mis-keyed
        # 'O Oliver'. That is the cross-source merge working, not a fan-out bug.
        # The PK no_duplicates on (source, source_id, league, season) + the
        # loader's unique-canonical_id check cover the real regressions.

        # Known-referee regression guard (issue #143): each anchor canonical_id
        # MUST carry rows from both sources. Offending = anchor present with
        # < 2 distinct sources → ERROR (cross-source merge silently broke).
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=0,
            where=(
                f"canonical_id IN ({known_csv}) AND canonical_id IN ("
                "SELECT canonical_id FROM iceberg.silver.xref_referee "
                "WHERE confidence = 'name_alias' "
                "GROUP BY canonical_id HAVING COUNT(DISTINCT source) < 2"
                ")"
            ),
            severity='ERROR',
            name='known_referee_pairs[xref_referee]',
        ),
    ]


#: Anchor canonical_ids for the manager 2-source regression guard (mirrors
#: KNOWN_REFEREE_CANONICALS). Long-tenured APL head coaches present in both
#: the FBref spine and at least one mirror source across recent seasons.
KNOWN_MANAGER_CANONICALS = (
    'mikel_arteta',
    'pep_guardiola',
    'unai_emery',
)


def build_xref_manager_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_manager`` — FBref spine + FotMob coachId
    mirror (#144) + Transfermarkt coach_id bridge (xref-improvements).

    Bounds sized for APL across ~10 seasons: ~30-50 distinct managers ×
    per-season presence × 3 sources ≈ 900 rows worst case — max 2000 keeps
    headroom; revisit per league onboarded. Per-source orphan-rate is
    evaluated separately by :func:`evaluate_orphan_rate_per_source` and
    appended by the DAG callable; TM-vs-FotMob dob disagreement by
    :func:`evaluate_manager_dob_collisions` (Phase 2.7).
    """
    table = 'iceberg.silver.xref_manager'
    checks = [
        CHECK.row_count(table, min_rows=20, max_rows=2000 * _in_scope_leagues()),

        CHECK.no_duplicates(
            table,
            pk=['source', 'source_id', 'league', 'season'],
        ),

        CHECK.no_nulls(table, cols=['canonical_id', 'source', 'source_id']),

        check_enum_compliance(
            table, 'source',
            allowed=['fbref', 'fotmob', 'transfermarkt'],
            severity='ERROR',
        ),

        # name_alias / name_initial added with the TM bridge (alias YAML tier
        # + surname-and-first-initial tier); see xref_manager.sql.j2 header.
        check_enum_compliance(
            table, 'confidence',
            allowed=['name_alias', 'name_normalize', 'name_initial', 'orphan'],
            severity='ERROR',
        ),

        # Collision guard (#144): two DIFFERENT FotMob coaches (distinct
        # coachId/source_id) whose names normalise to the same canonical_id
        # within one (league, season) would silently merge. FotMob rows are
        # unique on (source_id, league, season) by construction, so any dup of
        # (canonical_id, league, season) among them is a genuine name collision.
        # WARNING-only: rare youth/duplicate-name coaches shouldn't fail the DAG.
        CHECK.no_duplicates(
            table,
            pk=['canonical_id', 'league', 'season'],
            where="source = 'fotmob'",
            severity='WARNING',
            name='manager_collision[fotmob.canonical_id]',
        ),

        # Same guard for the TM bridge: two distinct coach_ids landing on one
        # canonical within (league, season) is a suspected false merge (most
        # likely a name_initial mis-fire) — WARNING, inspect + alias-fix.
        CHECK.no_duplicates(
            table,
            pk=['canonical_id', 'league', 'season'],
            where="source = 'transfermarkt'",
            severity='WARNING',
            name='manager_collision[transfermarkt.canonical_id]',
        ),

        # 3rd source presence: TM coaches must actually reach the table.
        # WARNING until the TM coaches backfill is confirmed on the target
        # env, then promote to ERROR.
        CHECK.row_count(
            table=table,
            min_rows=1,
            where="source = 'transfermarkt'",
            severity='WARNING',
            name='source_present[xref_manager.transfermarkt]',
        ),
    ]

    # Anchor regression (mirrors the referee anchors): each known manager
    # canonical must carry ≥2 distinct non-orphan sources. Implemented as
    # "0 offending anchors" via row_count over a per-anchor aggregate.
    anchors_in = ", ".join(f"'{cid}'" for cid in KNOWN_MANAGER_CANONICALS)
    checks.append(
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=0,
            where=(
                "canonical_id IN ("
                f"SELECT canonical_id FROM {table} "
                f"WHERE canonical_id IN ({anchors_in}) "
                "AND confidence <> 'orphan' "
                "GROUP BY canonical_id "
                "HAVING COUNT(DISTINCT source) < 2"
                f") AND canonical_id IN ({anchors_in})"
            ),
            severity='WARNING',
            name='known_manager_anchors[xref_manager]',
        )
    )
    return checks


def build_xref_player_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_player``.

    Confidence allow-list aligned with ``xref_player_resolver.py`` cascade.
    Orphan-rate per source is evaluated separately by
    :func:`evaluate_orphan_rate_per_source`; the results are appended to
    the run report by the DAG callable (see ``dag_transform_xref``).
    """
    table = 'iceberg.silver.xref_player'
    return [
        # T3 hotfix produced ~1500 rows for ENG-Premier League; lower bound
        # 400 stays conservative. Upper bound 50k allows multi-season growth.
        CHECK.row_count(table, min_rows=400, max_rows=50000 * _in_scope_leagues()),

        CHECK.no_duplicates(
            table,
            pk=['source', 'source_id', 'league', 'season'],
        ),

        CHECK.no_nulls(table, cols=['canonical_id', 'source', 'source_id']),

        # confidence — mirror the resolver cascade tier names verbatim.
        # 'name_team_surname' / 'name_team_subset' / 'name_team_nickname' /
        # 'name_team_alias' added by R2-followup v2 resolver. 'name_team_dob'
        # is live (DOB-corroboration adjudication of ambiguous rows);
        # 'name_team_jersey' remains a reserved STUB. 'ambiguous' is
        # INTENTIONALLY NOT in this list — Fellegi-Sunter clerical-review rows
        # must land in silver.xref_player_review, not xref_player. An
        # 'ambiguous' value here is therefore a DQ ERROR by design.
        check_enum_compliance(
            table, 'confidence',
            allowed=['exact', 'name_team', 'name_team_surname',
                     'name_team_subset', 'name_team_nickname',
                     'name_team_alias', 'name_team_jersey',
                     'name_team_dob', 'orphan'],
            severity='ERROR',
        ),

        # source enum — 9 sources (issue #43 added Transfermarkt + Capology
        # on top of FBref / Understat / WhoScored / FotMob / SofaScore; #601
        # added SoFIFA; #692 added ESPN lineups).
        check_enum_compliance(
            table, 'source',
            allowed=['fbref', 'understat', 'whoscored', 'fotmob', 'sofascore',
                     'transfermarkt', 'capology', 'sofifa', 'espn'],
            severity='ERROR',
        ),

        # canonical_id format guard — must start with one of the 9 known
        # prefixes (fb_/us_/ws_/fm_/ss_/tm_/cap_/sf_/es_); see
        # xref_player_resolver._orphan_prefix. Regex via Trino regexp_like;
        # we express this as a row_count of offending rows.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=0,
            where="NOT regexp_like(canonical_id, '^(fb|us|ws|fm|ss|tm|cap|sf|es)_.+$')",
            severity='ERROR',
            name='canonical_id_format[xref_player]',
        ),

        # Issue #70: prevent the fan-out pattern that prompted the Gold
        # ROW_NUMBER hack. A single canonical_id legitimately has one
        # *player* per (source, league, season); >1 distinct player means a
        # Gold JOIN on (source, source_id) without (league, season) will
        # fan-out 2×. Dedup is enforced in
        # xref_player_resolver._dedup_canonical_per_season; this gate makes
        # regressions visible.
        #
        # #803: count DISTINCT *player identity*, not source_id. ESPN's
        # source_id is the '<name>|<team>' composite, so one player with two
        # club-stints in a season (legit transfer, #720) is two source_ids on
        # one canonical — NOT a fan-out. split_part(source_id,'|',1) collapses
        # the team suffix to the player name; for every other source source_id
        # has no '|' so split_part is a no-op (returns it unchanged). The check
        # therefore fires only on TRUE different-player collisions.
        CHECK.row_count(
            table=table,
            min_rows=0,
            max_rows=0,
            where=(
                "(canonical_id, source, league, season) IN ("
                "SELECT canonical_id, source, league, season "
                "FROM iceberg.silver.xref_player "
                "WHERE confidence <> 'orphan' "
                "GROUP BY canonical_id, source, league, season "
                "HAVING COUNT(DISTINCT split_part(source_id, '|', 1)) > 1"
                ") AND confidence <> 'orphan'"
            ),
            severity='ERROR',
            name='no_duplicates_per_canonical_season[xref_player]',
        ),
    ]


def build_xref_player_review_checks() -> List[Check]:
    """DQ for ``iceberg.silver.xref_player_review`` (R2-followup v2 sibling).

    The review table holds Fellegi-Sunter clerical-review band rows — source
    candidates that the resolver flagged ambiguous (multiple candidates
    surfaced by surname-anchor / token_set / nickname tiers). It is intended
    to stay SMALL: a healthy resolver run produces ≤30 rows per (league,
    season). Significantly more rows is a signal that thresholds are
    miscalibrated or that the spine is missing entries.

    Checks:
      * row_count(min=0, max=200) — soft ceiling at 200 across the whole
        table. APL has 4 in-scope seasons × 3 sources, so 200 leaves
        meaningful headroom while still alarming on runaway growth.
      * no_duplicates on ``(source, source_id, league, season)`` — the
        resolver may legitimately surface the same source row across
        multiple seasons but never twice in the same season.
      * no_nulls on identifying columns.
      * enum compliance on ``rule`` — must match the rule labels emitted
        by the cascade (``surname_collision``, ``token_set_band``,
        ``nickname_collision``) plus the DOB-veto pass (``dob_veto`` —
        a fuzzy link whose Bronze DOB contradicts the canonical's
        consolidated DOB, demoted to review by the resolver).
      * enum compliance on ``source`` — six cascaded sources (everything
        except the FBref spine, which never lands in clerical review).
    """
    table = 'iceberg.silver.xref_player_review'
    return [
        CHECK.row_count(table, min_rows=0, max_rows=200 * _in_scope_leagues()),

        CHECK.no_duplicates(
            table,
            pk=['source', 'source_id', 'league', 'season'],
        ),

        CHECK.no_nulls(
            table,
            cols=['source', 'source_id', 'rule', 'league', 'season',
                  'detected_at'],
        ),

        check_enum_compliance(
            table, 'rule',
            allowed=['surname_collision', 'token_set_band',
                     'nickname_collision', 'dob_veto'],
            severity='ERROR',
        ),

        check_enum_compliance(
            table, 'source',
            allowed=['understat', 'whoscored', 'fotmob', 'sofascore',
                     'transfermarkt', 'capology', 'sofifa', 'espn'],
            severity='ERROR',
        ),
    ]


def build_all_xref_checks() -> List[Check]:
    """Aggregate DQ checks for all 5 xref tables + review sibling."""
    return (
        build_xref_team_checks()
        + build_xref_match_checks()
        + build_xref_referee_checks()
        + build_xref_manager_checks()
        + build_xref_player_checks()
        + build_xref_player_review_checks()
    )


# ---------------------------------------------------------------------------
# E1.5 post-cutover ref_integrity / canonical-format checks
# ---------------------------------------------------------------------------

def build_e1_5_post_cutover_checks() -> List[Check]:
    """Forward-looking DQ checks for the E1.5 cutover.

    These checks validate that Gold consumers correctly resolve to the new
    Silver xref source-of-truth. They are wired into Gold validate_gold_quality
    in the **prep PR** at severity=WARNING so we observe the diff during the
    ≥3-day green-parity gate-watch window without breaking the DAG. After
    cutover-merge a follow-up PR may tighten the team-level check to ERROR.

    Four checks (all severity=WARNING in this prep PR):

    1. ``ref_integrity[dim_team.team_id->silver.xref_team(fbref).canonical_id]``
       — Gold dim_team.team_id must trace back to silver.xref_team for the
       FBref source slice. Implemented as ``row_count(max=0)`` over the
       offending predicate because the universal ``CHECK.ref_integrity``
       has no WHERE-filter mode.

    2. ``ref_integrity[fct_player_match.team_id->dim_team]`` — narrow
       ref_integrity check that catches team_id slug drift introduced when
       SQL files cut over from gold.entity_xref → silver.xref_team.

    3-4. Canonical-format guards on ``dim_player.player_id`` and
        ``fct_player_match.player_id`` — both MUST start with ``fb_``
        post-cutover (FBref is the player spine). Implemented via
        ``regexp_like`` over a row_count(max=0) predicate.

    Severity rationale
    ------------------
    All four checks ship at WARNING during the gate-watch window (2026-05-09
    → 2026-05-12+). A non-zero offender count surfaces in Telegram via
    ``telegram_dq_summary`` without raising. After cutover the team-level
    check (#1) is the first candidate for ERROR-severity tightening — see
    ``docs/decisions/E1.5-cutover-prep.md``.

    NOTE: This builder does NOT modify ``dags/utils/data_quality.py`` —
    every check leverages an existing primitive (``row_count`` with WHERE).
    """
    checks: List[Check] = [
        # 1) dim_team.team_id ⊆ silver.xref_team.canonical_id (source='fbref')
        CHECK.row_count(
            table='iceberg.gold.dim_team',
            min_rows=0,
            max_rows=0,
            where=(
                "team_id NOT IN ("
                "SELECT canonical_id FROM iceberg.silver.xref_team "
                "WHERE source = 'fbref'"
                ")"
            ),
            severity='WARNING',
            name='ref_integrity[dim_team.team_id->silver.xref_team(fbref)]',
        ),

        # 2) fct_player_match.team_id ⊆ dim_team.team_id
        # issue #451: PR #438 переименовал team_id_canonical → team_id
        # (star-schema alignment) — предикат следует за схемой.
        CHECK.row_count(
            table='iceberg.gold.fct_player_match',
            min_rows=0,
            max_rows=0,
            where=(
                "team_id IS NOT NULL "
                "AND team_id NOT IN (SELECT team_id FROM iceberg.gold.dim_team)"
            ),
            severity='WARNING',
            name='ref_integrity[fct_player_match.team_id->dim_team]',
        ),

        # 3) dim_player.player_id matches '^fb_' regex
        CHECK.row_count(
            table='iceberg.gold.dim_player',
            min_rows=0,
            max_rows=0,
            where="NOT regexp_like(player_id, '^fb_.+')",
            severity='WARNING',
            name='canonical_format[dim_player.player_id]',
        ),

        # 4) fct_player_match.player_id matches '^fb_' regex
        # issue #451: PR #438 переименовал player_id_canonical → player_id
        # (star-schema alignment) — предикат следует за схемой.
        CHECK.row_count(
            table='iceberg.gold.fct_player_match',
            min_rows=0,
            max_rows=0,
            where="player_id IS NOT NULL AND NOT regexp_like(player_id, '^fb_.+')",
            severity='WARNING',
            name='canonical_format[fct_player_match.player_id]',
        ),
    ]
    return checks


# ---------------------------------------------------------------------------
# Orphan-rate evaluation (runs after the standard DQ pass)
# ---------------------------------------------------------------------------

def evaluate_orphan_rate_per_source(
    table: str = 'iceberg.silver.xref_player',
    warning_threshold: float = 10.0,
    error_threshold: float = 25.0,
    current_season_only: bool = False,
    group_by_league: bool = False,
) -> Dict[str, Any]:
    """Compute orphan-rate per ``source`` for an xref table and classify.

    Returns a dict::

        {
            'per_source': {
                'fbref': {'total': N, 'orphans': K, 'pct': X.X, 'verdict': 'OK'},
                ...
            },
            'overall_pct': float,
            'verdict': 'OK' | 'WARNING' | 'ERROR',
            'breaches': [{'source': str, 'pct': float, 'verdict': str}, ...],
        }

    Verdict semantics:
      * pct ≤ warning_threshold        — OK
      * warning < pct ≤ error          — WARNING
      * pct > error_threshold          — ERROR

    ``current_season_only`` (#803): when True, the rate is measured only on the
    latest season. After a historical backfill the table spans ~10 seasons whose
    old, thin FBref spine leaves most source rows legitimately orphan (#788) —
    table-wide that inflates the rate into a false ERROR (fotmob 28.8% /
    xref_team TM 86.9%), while the current season — where the spine is thick —
    is the meaningful resolver-health signal (1.3% / 0%). Sources absent in the
    latest season drop out of the GROUP BY and are simply not evaluated.

    ``group_by_league`` (multi-league prep): when True, rates are computed per
    ``(source, league)`` — ``per_source`` keys become ``'{source}|{league}'``
    and breaches carry a ``league`` field. The latest-season filter is then
    evaluated PER LEAGUE (a table-wide max season would mask a league whose
    freshest data is older). When False (default) the output is byte-identical
    to the historical single-league shape.

    NOTE: This function does NOT raise. Callers decide whether to escalate.
    """
    qualified = _qualify(table)
    if group_by_league:
        # Per-league latest season via a window — a table-wide max(season)
        # would silently exclude any league whose freshest partition is older.
        inner = (
            "SELECT source, league, confidence, season, "
            "MAX(season) OVER (PARTITION BY league) AS max_season "
            f"FROM {qualified}"
        )
        where = "WHERE season = max_season " if current_season_only else ""
        sql = (
            "SELECT source, league, "
            "       COUNT(*) AS total, "
            "       COUNT_IF(confidence = 'orphan') AS orphans "
            f"FROM ({inner}) "
            f"{where}"
            "GROUP BY source, league"
        )
    else:
        where = (
            f"WHERE season = (SELECT max(season) FROM {qualified}) "
            if current_season_only else ""
        )
        sql = (
            "SELECT source, "
            "       COUNT(*) AS total, "
            "       COUNT_IF(confidence = 'orphan') AS orphans "
            f"FROM {qualified} "
            f"{where}"
            "GROUP BY source"
        )
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()

    per_source: Dict[str, Dict[str, Any]] = {}
    breaches: List[Dict[str, Any]] = []
    overall_total = 0
    overall_orphans = 0
    overall_verdict = 'OK'

    for row in rows:
        if group_by_league:
            src, league, total, orphans = row
            key = f"{src}|{league}"
        else:
            src, total, orphans = row
            league = None
            key = src
        pct = (100.0 * orphans / total) if total else 0.0
        if pct > error_threshold:
            verdict = 'ERROR'
        elif pct > warning_threshold:
            verdict = 'WARNING'
        else:
            verdict = 'OK'

        per_source[key] = {
            'total': int(total),
            'orphans': int(orphans),
            'pct': round(pct, 2),
            'verdict': verdict,
        }
        if verdict != 'OK':
            breach = {'source': src, 'pct': round(pct, 2), 'verdict': verdict}
            if league is not None:
                breach['league'] = league
            breaches.append(breach)
        overall_total += int(total)
        overall_orphans += int(orphans)

        # Promote overall verdict to the strictest seen
        if verdict == 'ERROR':
            overall_verdict = 'ERROR'
        elif verdict == 'WARNING' and overall_verdict == 'OK':
            overall_verdict = 'WARNING'

    overall_pct = (100.0 * overall_orphans / overall_total) if overall_total else 0.0
    return {
        'per_source': per_source,
        'overall_pct': round(overall_pct, 2),
        'verdict': overall_verdict,
        'breaches': breaches,
    }


def report_orphan_teams(
    table: str = 'iceberg.silver.xref_team',
    limit: int = 200,
) -> Dict[str, Any]:
    """List the distinct un-aliased (``confidence='orphan'``) teams in an xref
    team table (issue #141, stage 4).

    Unlike :func:`evaluate_orphan_rate_per_source` (which yields *rates*), this
    yields the actual raw team-names that failed to glue, so a maintainer can
    extend ``team_aliases.yaml`` for exactly what is broken — e.g. a SofaScore
    "Liverpool FC" that never matched the "Liverpool" alias.

    Returns::

        {
            'total_orphans': int,         # distinct (source, source_id, league, season)
            'per_source': {'sofascore': 3, ...},
            'rows': [{'source', 'league', 'season', 'source_id'}, ...],  # ≤ limit
            'truncated': bool,
        }

    Informational only — never raises. Wire into the xref validation task and
    push to XCom / logs.
    """
    qualified = _qualify(table)
    sql = (
        "SELECT source, league, season, source_id "
        f"FROM {qualified} "
        "WHERE confidence = 'orphan' "
        "GROUP BY source, league, season, source_id "
        "ORDER BY source, league, season, source_id"
    )
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()

    per_source: Dict[str, int] = {}
    out_rows: List[Dict[str, Any]] = []
    for src, league, season, source_id in rows:
        per_source[src] = per_source.get(src, 0) + 1
        if len(out_rows) < limit:
            out_rows.append({
                'source': src,
                'league': league,
                'season': season,
                'source_id': source_id,
            })
    return {
        'total_orphans': len(rows),
        'per_source': per_source,
        'rows': out_rows,
        'truncated': len(rows) > limit,
    }


# ---------------------------------------------------------------------------
# Cross-source DOB conflicts (companion to the resolver's name_team_dob tier)
# ---------------------------------------------------------------------------

#: Source DOB projections: (source, SQL yielding (source_id, dob)). Most read
#: Bronze; WhoScored uses its manifest-backed Silver current view, which has no
#: xref dependency and therefore does not introduce a resolver cycle. Trino
#: dialect; tests inject DuckDB-compatible projections instead.
DEFAULT_PLAYER_DOB_PROJECTIONS = (
    ('fotmob',
     "SELECT CAST(player_id AS varchar) AS source_id, "
     "max_by(TRY_CAST(date_of_birth AS DATE), _ingested_at) AS dob "
     "FROM iceberg.bronze.fotmob_team_squad WHERE player_id IS NOT NULL "
     "GROUP BY CAST(player_id AS varchar)"),
    ('sofascore',
     "SELECT CAST(player_id AS varchar) AS source_id, "
     "max_by(TRY_CAST(date_of_birth AS DATE), _ingested_at) AS dob "
     "FROM iceberg.bronze.sofascore_player_profile WHERE player_id IS NOT NULL "
     "GROUP BY CAST(player_id AS varchar)"),
    ('transfermarkt',
     "SELECT CAST(player_id AS varchar) AS source_id, "
     "max_by(dob, _ingested_at) AS dob "
     "FROM iceberg.bronze.transfermarkt_players WHERE player_id IS NOT NULL "
     "GROUP BY CAST(player_id AS varchar)"),
    ('sofifa',
     "SELECT CAST(player_id AS varchar) AS source_id, "
     "max_by(TRY(CAST(date_parse(dob, '%b %e, %Y') AS DATE)), _ingested_at) AS dob "
     "FROM iceberg.bronze.sofifa_player_ratings "
     "WHERE player_id IS NOT NULL AND dob IS NOT NULL "
     "GROUP BY CAST(player_id AS varchar)"),
    ('whoscored',
     "SELECT CAST(player_id AS varchar) AS source_id, "
     "max_by(date_of_birth, fetched_at) AS dob "
     "FROM iceberg.silver.whoscored_player_profile_current "
     "WHERE player_id IS NOT NULL GROUP BY CAST(player_id AS varchar)"),
)


def evaluate_dob_conflicts(
    xref_table: str = 'iceberg.silver.xref_player',
    dob_projections=DEFAULT_PLAYER_DOB_PROJECTIONS,
    tolerance_days: int = 1,
    limit: int = 50,
) -> Dict[str, Any]:
    """Report canonical players whose linked sources disagree on birth date.

    Companion check to the resolver's DOB-corroboration tier: when two
    non-orphan sources bound to one ``fb_…`` canonical carry Bronze DOBs more
    than ``tolerance_days`` apart, at least one link is a suspected false
    merge (or one source's DOB is dirty). The resolver already EXCLUDES such
    canonicals from its own DOB map (they can neither veto nor promote), so
    this report is the only place the disagreement surfaces.

    Returns::

        {
            'conflicts': int,          # canonicals with a DOB spread > tolerance
            'rows': [{'canonical_id', 'min_dob', 'max_dob', 'spread_days',
                      'n_sources'}, ...],   # ≤ limit
            'truncated': bool,
            'verdict': 'OK' | 'WARNING',
        }

    Informational (WARNING-max) — never escalates to ERROR and lets the
    caller decide how to report. ``dob_projections`` is injectable for tests
    (the defaults use Trino-only TRY/date_parse).
    """
    qualified = _qualify(xref_table)
    union = "\nUNION ALL\n".join(
        f"SELECT '{src}' AS source, source_id, dob FROM ({proj})"
        for src, proj in dob_projections
    )
    sql = (
        f"WITH dob_src AS (\n{union}\n)\n"
        "SELECT x.canonical_id,\n"
        "       MIN(d.dob) AS min_dob,\n"
        "       MAX(d.dob) AS max_dob,\n"
        "       COUNT(DISTINCT d.source) AS n_sources\n"
        f"FROM {qualified} x\n"
        "JOIN dob_src d\n"
        "  ON d.source = x.source AND d.source_id = x.source_id\n"
        "WHERE x.confidence <> 'orphan'\n"
        "  AND x.canonical_id LIKE 'fb\\_%' ESCAPE '\\'\n"
        "  AND d.dob IS NOT NULL\n"
        "GROUP BY x.canonical_id\n"
        f"HAVING date_diff('day', MIN(d.dob), MAX(d.dob)) > {int(tolerance_days)}\n"
        "ORDER BY x.canonical_id"
    )
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()

    out_rows: List[Dict[str, Any]] = []
    for cid, min_dob, max_dob, n_sources in rows[:limit]:
        spread = (max_dob - min_dob).days if (min_dob and max_dob) else None
        out_rows.append({
            'canonical_id': cid,
            'min_dob': str(min_dob),
            'max_dob': str(max_dob),
            'spread_days': spread,
            'n_sources': int(n_sources),
        })
    return {
        'conflicts': len(rows),
        'rows': out_rows,
        'truncated': len(rows) > limit,
        'verdict': 'OK' if not rows else 'WARNING',
    }


def evaluate_manager_dob_collisions(
    xref_table: str = 'iceberg.silver.xref_manager',
    fotmob_profile_table: str = 'iceberg.silver.fotmob_manager_profile',
    tm_coaches_table: str = 'iceberg.silver.transfermarkt_coaches',
    tolerance_days: int = 1,
    limit: int = 50,
) -> Dict[str, Any]:
    """Report manager canonicals where FotMob and TM disagree on birth date.

    DOB corroboration for the manager bridge: a canonical carrying both a
    FotMob coachId and a TM coach_id whose profile DOBs differ by more than
    ``tolerance_days`` is a suspected false merge — the strongest signal for
    ``name_initial``-tier rows (the confidence of the TM row is included so
    the reviewer sees which tier produced the link). Candidates for a
    ``manager_aliases.yaml`` correction.

    Reads the two profile silver tables (not Bronze): unlike the player DOB
    maps this is NOT circular — fotmob_manager_profile / transfermarkt_coaches
    do not consume xref_manager.

    Returns ``{'collisions': N, 'rows': [...≤limit], 'truncated': bool,
    'verdict': 'OK'|'WARNING'}`` — never escalates to ERROR.
    """
    xq = _qualify(xref_table)
    fmq = _qualify(fotmob_profile_table)
    tmq = _qualify(tm_coaches_table)
    sql = (
        "WITH fm AS (\n"
        "    SELECT x.canonical_id, x.league, x.season,\n"
        "           MAX(TRY_CAST(p.date_of_birth AS DATE)) AS fm_dob\n"
        f"    FROM {xq} x\n"
        f"    JOIN {fmq} p\n"
        "      ON p.player_id = x.source_id\n"
        "     AND p.league = x.league AND p.season = x.season\n"
        "    WHERE x.source = 'fotmob' AND x.confidence <> 'orphan'\n"
        "    GROUP BY x.canonical_id, x.league, x.season\n"
        "),\n"
        "tm AS (\n"
        "    SELECT x.canonical_id, x.league, x.season, x.confidence,\n"
        "           MAX(c.dob) AS tm_dob\n"
        f"    FROM {xq} x\n"
        f"    JOIN {tmq} c\n"
        "      ON CAST(c.coach_id AS varchar) = x.source_id\n"
        "     AND c.league = x.league AND c.season = x.season\n"
        "    WHERE x.source = 'transfermarkt' AND x.confidence <> 'orphan'\n"
        "    GROUP BY x.canonical_id, x.league, x.season, x.confidence\n"
        ")\n"
        "SELECT tm.canonical_id, tm.league, tm.season, tm.confidence,\n"
        "       fm.fm_dob, tm.tm_dob\n"
        "FROM tm\n"
        "JOIN fm ON fm.canonical_id = tm.canonical_id\n"
        "       AND fm.league = tm.league AND fm.season = tm.season\n"
        "WHERE fm.fm_dob IS NOT NULL AND tm.tm_dob IS NOT NULL\n"
        f"  AND abs(date_diff('day', fm.fm_dob, tm.tm_dob)) > {int(tolerance_days)}\n"
        "ORDER BY tm.canonical_id, tm.season"
    )
    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        finally:
            cur.close()
    finally:
        conn.close()

    out_rows: List[Dict[str, Any]] = []
    for cid, league, season, confidence, fm_dob, tm_dob in rows[:limit]:
        out_rows.append({
            'canonical_id': cid,
            'league': league,
            'season': season,
            'tm_confidence': confidence,
            'fotmob_dob': str(fm_dob),
            'tm_dob': str(tm_dob),
        })
    return {
        'collisions': len(rows),
        'rows': out_rows,
        'truncated': len(rows) > limit,
        'verdict': 'OK' if not rows else 'WARNING',
    }


# ---------------------------------------------------------------------------
# Bronze-vs-xref freshness gap (Issue #15 regression guard)
# ---------------------------------------------------------------------------

#: Default Bronze tables consulted by :func:`evaluate_bronze_xref_freshness_gap`.
#: Each entry: (source_label, fully_qualified_bronze_table).
DEFAULT_FRESHNESS_BRONZE_TABLES = (
    ('understat', 'iceberg.bronze.understat_players'),
    ('fotmob', 'iceberg.bronze.fotmob_player_stats'),
)


def evaluate_bronze_xref_freshness_gap(
    bronze_tables=DEFAULT_FRESHNESS_BRONZE_TABLES,
    xref_table: str = 'iceberg.silver.xref_player',
    warning_lag_hours: float = 24.0,
    error_lag_hours: float = 72.0,
) -> Dict[str, Any]:
    """Compare Bronze player-table freshness against xref_player snapshot age.

    Issue #15 regression guard. ``silver.xref_player`` is materialised via a
    full DROP+CREATE+INSERT by :mod:`utils.xref_player_resolver`. If the
    resolver DAG (``dag_transform_xref``) is paused or stalls, recently-ingested
    Bronze players are silently absent from xref → downstream Gold facts get
    NULL canonical_id and orphan-rate metrics look healthy because the row
    never made it into the table at all (not even as an orphan).

    Symptom from Issue #15 (2026-05-17):
        * bronze.understat_players: 532 rows for season='2526' (incl. Bukayo Saka)
        * silver.xref_player: 267 rows for (understat, '2526') — Saka missing
        * Last resolver snapshot: 2026-05-15 14:54
        * Last understat 2526 Bronze ingest: 2026-05-17 09:00
        * Resulting Gold Understat coverage stuck at 50.30%

    Methodology
    -----------
    For each (source, bronze_table) pair we compute MAX(_ingested_at) per
    season and compare against MAX(committed_at) of the xref table's snapshot
    history. A positive lag means Bronze has data the resolver has not yet
    processed.

    Args:
        bronze_tables: Iterable of (source_label, qualified_bronze_table).
            Defaults to Understat + FotMob; WhoScored excluded because the
            resolver reads players from ``bronze.whoscored_events_current`` which is
            too large to scan freshness-per-season cheaply.
        xref_table: Iceberg table whose snapshot timestamp represents the
            last successful resolver run.
        warning_lag_hours: Lag above this — WARNING.
        error_lag_hours: Lag above this — ERROR.

    Returns:
        dict::

            {
                'xref_max_committed_at': datetime | None,
                'per_partition': [
                    {'source': str, 'season': str, 'bronze_max_ts': datetime,
                     'lag_hours': float, 'verdict': 'OK'|'WARNING'|'ERROR'},
                    ...
                ],
                'verdict': 'OK' | 'WARNING' | 'ERROR',
                'breaches': [...],  # entries with non-OK verdict
            }

    Does NOT raise — caller decides whether to escalate.
    """
    # Snapshot view name must NOT be sanitised by _safe_ident — it contains '$'.
    # We hardcode the schema/table parts; only the literal table name is
    # parameterised via xref_table.
    qualified = _qualify(xref_table)
    cat, schema, tbl = qualified.split('.')
    snapshots_view = f'{cat}.{schema}."{tbl}$snapshots"'

    conn = _get_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT MAX(committed_at) FROM {snapshots_view}"
            )
            row = cur.fetchone()
            xref_max = row[0] if row else None

            per_partition: List[Dict[str, Any]] = []
            for source_label, bronze_table in bronze_tables:
                bronze_qualified = _qualify(bronze_table)
                cur.execute(
                    "SELECT CAST(season AS varchar) AS season_str, "
                    "       MAX(_ingested_at) AS bronze_max "
                    f"FROM {bronze_qualified} "
                    "GROUP BY season"
                )
                for season_str, bronze_max in cur.fetchall():
                    if bronze_max is None or xref_max is None:
                        lag_hours = None
                    else:
                        delta = bronze_max - xref_max.replace(tzinfo=bronze_max.tzinfo)
                        lag_hours = delta.total_seconds() / 3600.0

                    if lag_hours is None or lag_hours <= 0:
                        verdict = 'OK'
                    elif lag_hours > error_lag_hours:
                        verdict = 'ERROR'
                    elif lag_hours > warning_lag_hours:
                        verdict = 'WARNING'
                    else:
                        verdict = 'OK'

                    per_partition.append({
                        'source': source_label,
                        'season': season_str,
                        'bronze_max_ts': bronze_max,
                        'lag_hours': (
                            round(lag_hours, 2) if lag_hours is not None else None
                        ),
                        'verdict': verdict,
                    })
        finally:
            cur.close()
    finally:
        conn.close()

    breaches = [p for p in per_partition if p['verdict'] != 'OK']
    if any(p['verdict'] == 'ERROR' for p in per_partition):
        overall = 'ERROR'
    elif any(p['verdict'] == 'WARNING' for p in per_partition):
        overall = 'WARNING'
    else:
        overall = 'OK'

    return {
        'xref_max_committed_at': xref_max,
        'per_partition': per_partition,
        'verdict': overall,
        'breaches': breaches,
    }
