"""
Player identity resolver — production-quality (E1, T3)
======================================================

Production port of the R2 spike prototype (``scripts/r2_resolver_proto.py``).
Resolves cross-source player identities into a single ``canonical_id`` and
materialises ``iceberg.silver.xref_player`` via Trino INSERT.

Pipeline
--------
1. Read Bronze for the FBref / Understat / WhoScored sources (Trino).
2. FBref is the spine — every FBref player becomes a canonical row
   (``canonical_id = 'fb_<player_id>'``, ``confidence = 'exact'``).
3. Understat / WhoScored cascade:

    - ``exact``     — when ``source_id`` matches an FBref player_id
      (rare cross-source).
    - ``name_team`` — fuzzy match on canonical-team bucket using
      ``rapidfuzz.fuzz.token_sort_ratio`` after ``unidecode + lower``;
      threshold ≥ 90.
    - ``name_team_dob`` — cross-source DOB corroboration: an ambiguous
      candidate (surname collision / token_set 88-94 band) is promoted when
      its Bronze date-of-birth uniquely matches the DOB consolidated for one
      FBref canonical from already-linked sources (see
      :func:`build_canonical_dob_map`). The FBref spine itself carries no
      DOB, so this is corroboration between non-FBref sources, never a
      direct spine comparison.
    - ``name_team_jersey`` — STUB (Bronze does not carry jersey
      consistently). Reserved in schema.
    - ``orphan`` — no match: ``canonical_id = '<src>_' || source_id`` where
      ``src`` ∈ {``us``, ``ws``, ``ss``}. ``ss`` (SofaScore) reserved for
      R0.2 follow-up.

4. Idempotent rewrite of ``iceberg.silver.xref_player``: DROP + CREATE +
   batched INSERT (500 rows per VALUES tuple to stay within Trino SQL
   length limits).
5. Known-pair regression guard — 10 hand-picked APL 2024-25 players must
   resolve into a single ``canonical_id`` across FBref/Understat/WhoScored
   (SofaScore intentionally excluded from regression at T6). Pass-rate
   < 8/10 raises :class:`ResolverError`.

Why Python (not pure SQL)?
--------------------------
``rapidfuzz.fuzz.token_sort_ratio`` and ``unidecode`` cannot be expressed
in Trino SQL. Total input on APL 2024-25 is ~1700 rows (562 FBref + 562
Understat + 491 WhoScored), so an in-memory pass is trivial — we only use
Trino for IO, never for the fuzzy matching itself.

Why no dependency on ``scrapers/*``?
------------------------------------
Importing ``scrapers/__init__.py`` from an Airflow task pulls in
nodriver / selenium / soccerdata / curl_cffi, which would push DAG-parse
RAM to ~1.5 GB and break the scheduler memory cap. This module imports
only ``trino`` (DBAPI) at module level and lazy-imports ``rapidfuzz`` /
``unidecode`` inside :func:`run_resolver` so DAG-parse stays cheap.

Public API contract (frozen for T4 DAG integration)
---------------------------------------------------
* :func:`run_resolver` — full pipeline; returns a summary dict.
* :func:`normalize_name` — pure helper; testable without Trino.
* :func:`canonical_team_for_resolver` — wrapper over
  :func:`utils.medallion_config.get_canonical_team_name` with a sensible
  fallback (raw-name passthrough) so unmapped clubs still get *some* team
  bucket and aren't silently dropped from the cascade.
* :func:`cascade_resolve` — pure tier-cascade evaluator over an in-memory
  spine index. Used both by :func:`run_resolver` and by unit tests.
* :class:`ResolverError` — raised on regression (known-pair pass-rate
  below threshold).
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import trino as trino_lib

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module constants
# ---------------------------------------------------------------------------
DEFAULT_TARGET_TABLE = 'iceberg.silver.xref_player'

#: Iceberg sibling table for the Fellegi-Sunter clerical-review band — rows
#: that the v2 cascade flagged as ambiguous (multiple equally-good candidates
#: in a (season, canonical_team) bucket, or token_set scores in 88-94 grey
#: zone) land here, NEVER in xref_player. Maintained by the same resolver run.
DEFAULT_REVIEW_TABLE = 'iceberg.silver.xref_player_review'

#: token_sort_ratio threshold (0-100). 90 was tuned on the R2 algo spike —
#: catches typical accent / dash / order-of-tokens variants without leaking
#: cross-player false positives. Do NOT lower without rerunning the spike.
NAME_THRESHOLD: float = 90.0

#: token_set_ratio thresholds for the v2 R2-followup cascade tier.
#: ≥ TOKEN_SET_AUTO     — auto-link with confidence='name_team_subset'
#: TOKEN_SET_BAND_LOW   ≤ score < TOKEN_SET_AUTO  → ambiguous (review queue)
#: Below TOKEN_SET_BAND_LOW — no signal, fall through to next tier.
TOKEN_SET_AUTO: float = 95.0
TOKEN_SET_BAND_LOW: float = 88.0

#: Maximum Levenshtein distance allowed on the surname (last token) for
#: tier 2.3 surname-anchor matching. 1 catches typical typos (Roberson↔Robertson)
#: without leaking distinct surnames into each other.
SURNAME_LEVENSHTEIN_MAX: int = 1

#: Minimum surname length for tier 2.3. Below 4 chars Levenshtein≤1 produces
#: too many false matches (Cole/Cone/Cool, Saka/Sako).
SURNAME_MIN_LEN: int = 4

#: DOB corroboration tolerance in days. ±1 absorbs off-by-one dates from
#: timezone-shifted feeds (SofaScore stores an epoch upstream); a difference
#: > 1 day is treated as "different person". Missing DOB on either side is
#: NO signal — the row falls through unchanged (strictly additive feature).
DOB_TOLERANCE_DAYS: int = 1

#: Tiers trusted to seed the per-canonical DOB map (near-zero false-positive
#: rate). ``exact`` = cross-source id match; ``name_team_alias`` = human-curated
#: YAML; ``name_team`` = token_sort_ratio ≥ 90. Weaker fuzzy tiers are excluded
#: so a surname-tier false match cannot poison the map with its own DOB.
DOB_ANCHOR_TIERS: Tuple[str, ...] = ('exact', 'name_team_alias', 'name_team')

#: Fuzzy tiers subject to the DOB veto: when the candidate row carries a DOB
#: AND the canonical's consolidated DOB is known AND they differ by more than
#: DOB_TOLERANCE_DAYS, the link is demoted to the review queue (rule
#: 'dob_veto') — a wrong link is worse than a missing one. ``exact`` and
#: ``name_team_alias`` are never vetoed (authoritative). ``name_team`` IS
#: vetoable even though it also anchors the map: a row can never contradict
#: its own contribution, and a two-source disagreement excludes the canonical
#: from the map entirely (surfaced via the dob_conflicts DQ report instead).
DOB_VETO_TIERS: Tuple[str, ...] = (
    'name_team', 'name_team_surname', 'name_team_subset', 'name_team_nickname',
)

#: Sources covered. ``sofifa`` bridges on the standard name+team cascade after
#: mapping its FIFA/FC edition to a football-season slug (``'FC 26'`` -> ``'2526'``,
#: see :func:`_fetch_sofifa_players`) — the edition/season mismatch is handled at
#: read time, not by a dedicated tier.
SOURCES: Tuple[str, ...] = (
    'fbref', 'understat', 'whoscored', 'fotmob', 'sofascore',
    'transfermarkt', 'capology', 'sofifa', 'espn',
)

#: Default batch size for ``INSERT INTO ... VALUES (...)``. 500 fits
#: comfortably under Trino's default ``query.max-length`` (≈ 16 MB) for
#: our per-row payload size (~150 bytes).
DEFAULT_CHUNK_SIZE = 500

#: Known pairs per league that the resolver MUST resolve to a single
#: canonical_id across the core sources. Pulled from the R2 spike — hard-coded
#: rather than configurable so a regression in alias or threshold tuning
#: surfaces immediately. Kept in sync with ``scripts/r2_resolver_proto.py``
#: (KNOWN_PAIRS at the bottom). A league with NO entry here SKIPS the
#: regression gate with a WARNING (multi-league prep) — add anchors when a
#: league is onboarded, do not fail it on an empty anchor set.
KNOWN_PAIRS_BY_LEAGUE: Dict[str, Tuple[Tuple[str, str], ...]] = {
    'ENG-Premier League': (
        ('Bukayo Saka', 'fb_bc7dc64d'),
        ('Mohamed Salah', 'fb_e342ad68'),
        ('Erling Haaland', 'fb_1f44ac21'),
        ('Bruno Fernandes', 'fb_507c7bdf'),
        ('Rodri', 'fb_6434f10d'),
        ('Son Heung-min', 'fb_92e7e919'),
        ('Virgil van Dijk', 'fb_e06683ca'),
        ('Cole Palmer', 'fb_dc7f8a28'),
        ('Bruno Guimarães', 'fb_82518f62'),
        ('Joško Gvardiol', 'fb_5ad50391'),
    ),
}

#: Back-compat alias — the APL anchor set (tests and the proto script refer
#: to the flat tuple).
KNOWN_PAIRS: Tuple[Tuple[str, str], ...] = (
    KNOWN_PAIRS_BY_LEAGUE['ENG-Premier League']
)

#: Below this pass-rate the resolver raises ResolverError. 8/10 is the
#: target codified in docs/research/R2_player_resolver.md.
KNOWN_PAIR_MIN_PASS = 8

#: Extended known-pair gate over FBref+SofaScore+FotMob. WARNING-only for
#: now: the historical exclusion reason (sparse sofascore_player_profile
#: names) must be empirically disproven on live data before this can raise
#: ResolverError like the core gate. TODO(#xref-dob): promote to a hard gate
#: after observing the live pass-rate for a few green runs.
KNOWN_PAIR_EXT_MIN_PASS = 8


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class ResolverError(RuntimeError):
    """Raised when the resolver fails a regression / quality gate.

    The intent is for an Airflow task to translate this into a failed
    DAG-run rather than silently producing a degraded ``xref_player`` table.
    """


# ---------------------------------------------------------------------------
# Pure helpers (testable without Trino)
# ---------------------------------------------------------------------------
def normalize_name(s: Optional[str]) -> str:
    """Normalize a player name for fuzzy comparison.

    ``unidecode`` strips diacritics ("Joško" -> "Josko"), then we lowercase
    and collapse whitespace. Lazy-imported because ``unidecode`` is ~2 MB and
    not needed by Airflow's DAG-parse step.

    Args:
        s: Raw name. ``None`` and empty string both return ``""``.

    Returns:
        Normalized form. Order of tokens is NOT canonicalized here —
        the cascade's fuzzy matching uses ``token_sort_ratio`` which handles
        token-order invariance (so ``"Heung-Min Son"`` and ``"Son Heung-min"``
        score 100 after normalisation).
    """
    if not s:
        return ""
    # Lazy import — keeps DAG-parse <50ms and avoids unidecode loading
    # its 600 KB data table at import time.
    from unidecode import unidecode  # type: ignore

    return " ".join(unidecode(s).lower().split())


# FotMob youth squads ("Arsenal U21", "Chelsea Under-21") — the FBref APL spine
# never carries U18-U23 teams, so these rows can only ever orphan. Matched on
# the raw team name and excluded at ingest (issue #563).
_YOUTH_TEAM_RE = re.compile(r"(?i)\b(?:under[-\s]?|u[-\s]?)(?:18|19|20|21|23)\b")


def _is_youth_team(team_name: Optional[str]) -> bool:
    """True for FotMob youth squads (e.g. 'Arsenal U21') — never in FBref spine."""
    return bool(team_name and _YOUTH_TEAM_RE.search(team_name))


def canonical_team_for_resolver(
    raw_team: Optional[str],
    source: str,
) -> Optional[str]:
    """Resolve a raw team name via :mod:`utils.medallion_config`.

    Behaviour:
      * Empty / None input  -> returns ``None``.
      * Mapped raw -> returns canonical_name (e.g. "Wolves" -> "Wolverhampton
        Wanderers").
      * Unmapped raw -> returns the raw_name *stripped* (acts as identity).
        This is intentional: an unmapped club still groups its own players
        into a single bucket, so a within-team fuzzy lookup still works
        even if the alias YAML missed an entry. Cross-source mismatches
        ("Wolves" vs "Wolverhampton Wanderers") obviously won't reconcile,
        but they would also fail in the legacy SQL flow — same blast radius.

    Args:
        raw_team: As stored in Bronze (e.g. ``Spurs``, ``Wolverhampton``).
        source: One of :data:`SOURCES` — passed through to
            ``medallion_config.get_canonical_team_name`` so the right
            per-source alias bucket is consulted (plus ``_generic``).
    """
    if not raw_team:
        return None
    raw = raw_team.strip()
    if not raw:
        return None
    # Lazy import — medallion_config opens YAML on first call.
    from utils.medallion_config import get_canonical_team_name

    canonical = get_canonical_team_name(raw, source=source)
    if canonical:
        return canonical
    # #836: some sources carry an official "<name> FC" / "<name> AFC" suffix that
    # the alias map keys without (Transfermarkt: "Arsenal FC", "Sunderland AFC",
    # "Liverpool FC" — 11 APL clubs). Without normalization the whole roster lands
    # in a "<name> FC" bucket that never meets the FBref spine's "<name>" bucket,
    # so every player orphans. Strip a trailing FC/AFC and retry before falling
    # back to identity. ("AFC Bournemouth" keeps its leading AFC — anchored to $.)
    stripped = re.sub(r'\s+A?FC$', '', raw).strip()
    if stripped and stripped != raw:
        canonical = get_canonical_team_name(stripped, source=source)
        return canonical if canonical else stripped
    return raw


# ---------------------------------------------------------------------------
# Spine index + cascade
# ---------------------------------------------------------------------------
class _FBrefSpine:
    """In-memory FBref player index, keyed by (season, canonical team).

    Two lookup paths:
      * ``by_id``       — exact match on FBref ``player_id`` (season-agnostic;
        first-seen row wins, since canonical_id is per-player not per-season).
      * ``by_team``     — list of (normalized_name, player_id) pairs per
        ``(season, canonical_team)`` bucket. Multi-season: a player who
        moved clubs (e.g. Cole Palmer Man City->Chelsea) appears in both
        ``('2324', 'Manchester City')`` and ``('2425', 'Chelsea')`` buckets.
      * ``norm_to_id``  — keyed by ``(name_norm, season, canonical_team)``.
    """

    __slots__ = ('by_id', 'by_team', 'norm_to_id')

    def __init__(self, fbref_rows: List[Dict[str, Any]]):
        # row keys: 'player_id', 'player_name', 'canonical_team' (already
        # canonicalised by caller — keeps cascade pure of YAML access).
        self.by_id: Dict[str, Dict[str, Any]] = {}
        self.by_team: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        self.norm_to_id: Dict[Tuple[str, str, str], str] = {}

        for row in fbref_rows:
            pid = row['player_id']
            if not pid:
                continue
            if pid not in self.by_id:
                self.by_id[pid] = row
            team = row.get('canonical_team') or ''
            season = row.get('season') or ''
            n = normalize_name(row.get('player_name'))
            if team and season:
                bucket = self.by_team.setdefault((season, team), [])
                if any(b_pid == pid for _, b_pid in bucket):
                    continue
                bucket.append((n, pid))
                self.norm_to_id[(n, season, team)] = pid

    def find_by_id(self, source_id: str) -> Optional[str]:
        """Tier-1: exact FBref player_id match."""
        return source_id if source_id in self.by_id else None

    def find_by_name_team(
        self,
        name: Optional[str],
        canonical_team: Optional[str],
        season: Optional[str] = None,
    ) -> Tuple[Optional[str], float]:
        """Tier-2: fuzzy name match within a (season, canonical-team) bucket.

        Returns ``(player_id, score)`` if best score ≥ :data:`NAME_THRESHOLD`,
        else ``(None, best_score_seen)`` — the raw best score is preserved
        so orphan rows can still record "we tried, here's how close it got"
        for downstream debugging.
        """
        if not canonical_team or not season:
            return None, 0.0
        cands = self.by_team.get((season, canonical_team))
        if not cands:
            return None, 0.0

        n = normalize_name(name)
        if not n:
            return None, 0.0

        # Lazy import inside the hot path — but only on first call per
        # process. Subsequent calls benefit from import cache.
        from rapidfuzz import fuzz  # type: ignore

        best_id: Optional[str] = None
        best_score = 0.0
        for cn, fid in cands:
            score = float(fuzz.token_sort_ratio(n, cn))
            if score > best_score:
                best_id, best_score = fid, score
        if best_score >= NAME_THRESHOLD:
            return best_id, best_score
        return None, best_score

    # -----------------------------------------------------------------
    # v2 cascade tiers (R2-followup)
    # -----------------------------------------------------------------
    # Each tier method returns a (player_id, score, candidates) triple
    # where ``candidates`` is the list of (player_id, name, score) tuples
    # that satisfied the tier rule. Convention:
    #   * exactly 1 candidate  -> player_id is set, candidates has 1 item
    #   * >1 candidates        -> player_id=None, score=0.0, ambiguous
    #   * 0 candidates / no-op -> player_id=None, score=0.0, candidates=[]
    # The "ambiguous" / "no-op" cases are distinguishable by ``len(candidates)``.

    def find_by_surname(
        self,
        name: Optional[str],
        canonical_team: Optional[str],
        season: Optional[str] = None,
    ) -> Tuple[Optional[str], float, List[Tuple[str, str, float]]]:
        """Tier 2.3: surname-anchor with Levenshtein≤1 on the last token.

        Source name's final token must equal the FBref candidate's final
        token, OR be within :data:`SURNAME_LEVENSHTEIN_MAX` edit distance
        (only when both surnames have ≥ :data:`SURNAME_MIN_LEN` chars —
        below that threshold edit-distance is too forgiving).

        Uniqueness guard: > 1 surnames-match in the same bucket → ambiguous.
        """
        if not canonical_team or not season:
            return None, 0.0, []
        cands = self.by_team.get((season, canonical_team))
        if not cands:
            return None, 0.0, []

        n = normalize_name(name)
        if not n:
            return None, 0.0, []
        n_tokens = n.split()
        if not n_tokens:
            return None, 0.0, []
        n_last = n_tokens[-1]
        if len(n_last) < SURNAME_MIN_LEN:
            return None, 0.0, []

        from rapidfuzz.distance import Levenshtein  # type: ignore

        matches: List[Tuple[str, str, float]] = []
        for cn, fid in cands:
            cn_tokens = cn.split()
            if not cn_tokens:
                continue
            cn_last = cn_tokens[-1]
            if n_last == cn_last:
                matches.append((fid, cn, 100.0))
            elif (
                len(cn_last) >= SURNAME_MIN_LEN
                and Levenshtein.distance(n_last, cn_last) <= SURNAME_LEVENSHTEIN_MAX
            ):
                matches.append((fid, cn, 99.0))

        if len(matches) == 1:
            fid, _cn, score = matches[0]
            return fid, score, matches
        return None, 0.0, matches

    def find_by_token_set(
        self,
        name: Optional[str],
        canonical_team: Optional[str],
        season: Optional[str] = None,
    ) -> Tuple[Optional[str], float, List[Tuple[str, str, float]], str]:
        """Tier 2.5 / 2.6: ``token_set_ratio`` with two thresholds.

        Returns ``(player_id, score, candidates, kind)`` where ``kind`` is:

        * ``'auto'``           — exactly one candidate scored ≥ TOKEN_SET_AUTO.
        * ``'ambiguous_band'`` — candidates exist in either the auto-link OR
          the band 88-94 zone, but not a unique 95+ match (Fellegi-Sunter
          clerical-review band — caller must NOT auto-merge).
        * ``'none'``           — no candidate scored ≥ TOKEN_SET_BAND_LOW.

        ``token_set_ratio`` returns 100 when one string is a token-subset of
        the other ("Pape Sarr" ⊂ "Pape Matar Sarr"), so this tier closes the
        single most-impactful failure mode of the v1 resolver (subset names).
        """
        if not canonical_team or not season:
            return None, 0.0, [], 'none'
        cands = self.by_team.get((season, canonical_team))
        if not cands:
            return None, 0.0, [], 'none'

        n = normalize_name(name)
        if not n:
            return None, 0.0, [], 'none'

        from rapidfuzz import fuzz  # type: ignore

        auto: List[Tuple[str, str, float]] = []
        band: List[Tuple[str, str, float]] = []
        for cn, fid in cands:
            score = float(fuzz.token_set_ratio(n, cn))
            if score >= TOKEN_SET_AUTO:
                auto.append((fid, cn, score))
            elif score >= TOKEN_SET_BAND_LOW:
                band.append((fid, cn, score))

        if len(auto) == 1:
            fid, _cn, score = auto[0]
            return fid, score, auto, 'auto'
        if auto:
            # >1 candidates at 95+ — ambiguous
            return None, 0.0, auto, 'ambiguous_band'
        if band:
            # 88-94 — Fellegi-Sunter clerical-review band, never auto-link
            return None, 0.0, band, 'ambiguous_band'
        return None, 0.0, [], 'none'

    def find_by_nickname(
        self,
        name: Optional[str],
        canonical_team: Optional[str],
        season: Optional[str] = None,
        nn: Any = None,
    ) -> Tuple[Optional[str], List[Tuple[str, str, float]]]:
        """Tier 2.7: ``nicknames`` PyPI dict over first-name pairs.

        Asymmetric Splink #2206 rule: A's first-name in ``nn.nicknames_of(B)``
        OR vice versa, OR canonical-of pair. Surnames MUST already match
        (last token equality) — without this guard the tier produces
        cross-player matches via common nicknames.
        """
        if nn is None or not canonical_team or not season:
            return None, []
        cands = self.by_team.get((season, canonical_team))
        if not cands:
            return None, []

        n = normalize_name(name)
        if not n:
            return None, []
        n_tokens = n.split()
        if len(n_tokens) < 2:
            return None, []
        n_first, n_last = n_tokens[0], n_tokens[-1]

        matches: List[Tuple[str, str, float]] = []
        for cn, fid in cands:
            cn_tokens = cn.split()
            if len(cn_tokens) < 2:
                continue
            cn_first, cn_last = cn_tokens[0], cn_tokens[-1]
            if n_last != cn_last:
                continue
            if _nickname_match(n_first, cn_first, nn):
                matches.append((fid, cn, 100.0))

        if len(matches) == 1:
            return matches[0][0], matches
        return None, matches


def _nickname_match(a: str, b: str, nn: Any) -> bool:
    """Asymmetric ``nicknames``-pkg pair check (Splink discussion #2206 rule).

    True if either ``a`` is a nickname / canonical of ``b``, or vice versa.
    Equality short-circuits to True. The ``nicknames`` package emits names
    in title-case ({"Andrew", "Andre"}), while the resolver normalises
    everything to lower-case via :func:`normalize_name` — so we case-fold
    the package's output before comparison. Empty / unknown lookup yields
    an empty set, which falls through cleanly to the next branch.

    Args:
        a, b: normalised lower-case first names.
        nn: a ``nicknames.NickNamer`` instance (caller manages lifecycle).
    """
    if a == b:
        return True
    if a in {x.lower() for x in nn.nicknames_of(b)}:
        return True
    if b in {x.lower() for x in nn.nicknames_of(a)}:
        return True
    if a in {x.lower() for x in nn.canonicals_of(b)}:
        return True
    if b in {x.lower() for x in nn.canonicals_of(a)}:
        return True
    return False


def _alias_lookup(
    source: str,
    source_id: str,
    season: Optional[str],
) -> Optional[str]:
    """Tier-3 fallback: hand-curated ``player_aliases.yaml`` override.

    Returns the FBref player_id (without ``fb_`` prefix) or None when no
    entry covers ``(source, source_id, season)``. Lazy-imports
    :mod:`utils.medallion_config` so DAG-parse cost stays cheap.

    A missing YAML or empty list is the common case at E1 — the tier
    simply no-ops.
    """
    if not source or not source_id:
        return None
    from utils.medallion_config import get_player_alias  # lazy

    return get_player_alias(source, str(source_id), str(season or ''))


def cascade_resolve(
    candidate: Dict[str, Any],
    spine: _FBrefSpine,
    *,
    nn: Any = None,
    ambiguity_out: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[str], str, Optional[float]]:
    """Run the v2 tier cascade for a single non-FBref candidate row.

    Args:
        candidate: dict with keys ``source``, ``source_id``, ``player_name``,
            ``canonical_team`` (and ``season`` for the per-season buckets).
        spine: prebuilt :class:`_FBrefSpine`.
        nn: optional ``nicknames.NickNamer`` instance to enable tier 2.7.
            When ``None`` the nickname tier is skipped (the existing legacy
            test suite passes ``nn=None`` so behaviour stays unchanged for
            non-nickname kingpins).
        ambiguity_out: optional dict mutated by the cascade when a tier
            returns ambiguous (multiple candidates in the same bucket).
            Keys populated: ``rule``, ``candidates`` (list of
            ``(player_id, name, score)`` tuples), ``best_score``. When
            ``None`` (default) ambiguous candidates simply produce
            ``(None, 'ambiguous', None)`` and the caller decides whether
            to escalate or drop.

    Returns:
        ``(canonical_id, confidence, match_score)``.

        * ``confidence='exact'``               — ``match_score`` is None.
        * ``confidence='name_team'``           — token_sort_ratio≥90 win.
        * ``confidence='name_team_surname'``   — surname-anchor + Levenshtein≤1.
        * ``confidence='name_team_subset'``    — token_set_ratio≥95.
        * ``confidence='name_team_nickname'``  — first-name nickname pair
                                                + surname match.
        * ``confidence='name_team_alias'``     — player_aliases.yaml lookup
                                                (Tier 1.5, ranks above the
                                                fuzzy/ambiguous tiers).
        * ``confidence='orphan'``              — no tier matched. ``match_score``
                                                is best score seen at tier-2.
        * ``confidence='ambiguous'``           — a fuzzy/dict tier had >1
                                                candidate; ``canonical_id`` is
                                                ``None`` and the row must
                                                land in ``xref_player_review``.

    Pure function over ``(candidate, spine, nn, ambiguity_out)`` so unit
    tests can assert exact behaviour without any Trino mock.
    """
    src = candidate['source']
    sid = str(candidate['source_id'])
    name = candidate.get('player_name')
    team = candidate.get('canonical_team')
    season = candidate.get('season')

    # Tier-1: exact FBref-id match.
    fid = spine.find_by_id(sid)
    if fid:
        return f'fb_{fid}', 'exact', None

    # Tier 1.5: hand-curated player_aliases.yaml override. A human-curated
    # alias is authoritative and MUST beat every fuzzy/dict tier — those tiers
    # short-circuit on a wrong surname match or an ambiguous-band/collision
    # verdict before the cascade would otherwise reach an alias, so an alias
    # placed any later is unreachable for exactly the cases it is meant to fix
    # (#738: 'Bobby Reid' surname-collides into 'Harrison Reed'; 'Gabriel'
    # token_set_band is ambiguous across three real Gabriels).
    alias_pid = _alias_lookup(src, sid, season)
    if alias_pid:
        return f'fb_{alias_pid}', 'name_team_alias', 100.0

    # Tier-2: legacy token_sort_ratio ≥ NAME_THRESHOLD.
    fid, name_team_score = spine.find_by_name_team(name, team, season)
    if fid:
        return f'fb_{fid}', 'name_team', name_team_score

    # Tier 2.3: surname-anchor with Levenshtein on the last token.
    fid, surname_score, surname_cands = spine.find_by_surname(name, team, season)
    if fid:
        return f'fb_{fid}', 'name_team_surname', surname_score
    if len(surname_cands) > 1:
        if ambiguity_out is not None:
            ambiguity_out.update({
                'rule': 'surname_collision',
                'candidates': surname_cands,
                'best_score': max(c[2] for c in surname_cands),
            })
        return None, 'ambiguous', None

    # Tier 2.5 / 2.6: token_set_ratio (subset / 88-94 band).
    fid, ts_score, ts_cands, ts_kind = spine.find_by_token_set(name, team, season)
    if fid:
        return f'fb_{fid}', 'name_team_subset', ts_score
    if ts_kind == 'ambiguous_band':
        if ambiguity_out is not None:
            ambiguity_out.update({
                'rule': 'token_set_band',
                'candidates': ts_cands,
                'best_score': max((c[2] for c in ts_cands), default=0.0),
            })
        return None, 'ambiguous', None

    # Tier 2.7: nicknames dict (only if NickNamer instance was supplied).
    fid, nick_cands = spine.find_by_nickname(name, team, season, nn=nn)
    if fid:
        return f'fb_{fid}', 'name_team_nickname', 100.0
    if len(nick_cands) > 1:
        if ambiguity_out is not None:
            ambiguity_out.update({
                'rule': 'nickname_collision',
                'candidates': nick_cands,
                'best_score': 100.0,
            })
        return None, 'ambiguous', None

    # (alias is now Tier 1.5 above — it must outrank the fuzzy/ambiguous tiers.)

    # Else: orphan. Preserve best score from tier-2 (best fuzzy attempt).
    prefix = _orphan_prefix(src)
    return (
        f'{prefix}_{sid}',
        'orphan',
        name_team_score if name_team_score > 0 else None,
    )


def _orphan_prefix(source: str) -> str:
    """Map source name to orphan canonical_id prefix.

    Hard-coded — keeps tier-cascade insulated from string-mangling logic
    and means a typo in a source name surfaces as an immediate KeyError.
    """
    return {
        'understat':     'us',
        'whoscored':     'ws',
        'fotmob':        'fm',
        'sofascore':     'ss',
        'transfermarkt': 'tm',
        'capology':      'cap',
        'sofifa':        'sf',
        'espn':          'es',
    }[source]


# ---------------------------------------------------------------------------
# DOB corroboration (tier name_team_dob + dob_veto)
# ---------------------------------------------------------------------------
def _dob_close(a: Any, b: Any) -> bool:
    """True when both dates are present and within :data:`DOB_TOLERANCE_DAYS`.

    Missing on either side → False (callers must treat that as "no signal",
    not as a contradiction — see the veto predicate in :func:`_resolve_all`).
    """
    if a is None or b is None:
        return False
    return abs((a - b).days) <= DOB_TOLERANCE_DAYS


def build_canonical_dob_map(
    rows: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """Consolidate a per-canonical DOB from already-resolved non-FBref rows.

    Only rows whose confidence is in :data:`DOB_ANCHOR_TIERS`, whose
    canonical_id is a real FBref spine id (``fb_…``) and which carry an
    in-memory ``dob`` (annotated by :func:`run_resolver` from the Bronze DOB
    maps) contribute. Per canonical:

    * all observed DOBs pairwise within :data:`DOB_TOLERANCE_DAYS` → the
      canonical gets ``min(dates)`` (deterministic representative);
    * any wider disagreement → the canonical is EXCLUDED from the map and
      recorded in the returned conflict list — a poisoned/ambiguous DOB must
      neither veto nor promote anything.

    Returns ``(canonical_dob, dob_conflicts)`` where ``dob_conflicts`` items
    are ``{'canonical_id': ..., 'values': [(source, dob), ...]}``.

    Pure function — unit-testable without Trino.
    """
    from collections import defaultdict

    observed: Dict[str, List[Tuple[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get('source') == 'fbref':
            continue
        if row.get('confidence') not in DOB_ANCHOR_TIERS:
            continue
        cid = row.get('canonical_id') or ''
        if not cid.startswith('fb_'):
            continue
        dob = row.get('dob')
        if dob is None:
            continue
        observed[cid].append((row['source'], dob))

    canonical_dob: Dict[str, Any] = {}
    conflicts: List[Dict[str, Any]] = []
    for cid, pairs in observed.items():
        dates = [d for _, d in pairs]
        if all(_dob_close(a, b) for a in dates for b in dates):
            canonical_dob[cid] = min(dates)
        else:
            conflicts.append({'canonical_id': cid, 'values': pairs})
    return canonical_dob, conflicts


def adjudicate_ambiguous_with_dob(
    candidate: Dict[str, Any],
    ambiguity_info: Dict[str, Any],
    canonical_dob: Dict[str, Any],
) -> Optional[Tuple[str, str, float]]:
    """Try to resolve an ambiguous candidate via DOB corroboration.

    Among the ambiguity candidates (``(fbref_id, name, score)`` tuples from
    the cascade), keep those whose consolidated canonical DOB matches the
    candidate's Bronze DOB within tolerance. Promote **iff exactly one**
    matches — spine candidates with an unknown DOB do not block the unique
    match (the residual "namesake sharing a birthday inside one club+season"
    risk is negligible). ``match_score`` is the matching candidate's own tier
    score so the name-signal strength stays visible; the DOB corroboration is
    expressed by the ``name_team_dob`` confidence label.

    Returns ``(canonical_id, 'name_team_dob', match_score)`` or ``None``.

    Pure function — unit-testable without Trino.
    """
    dob = candidate.get('dob')
    if dob is None:
        return None
    cands: List[Tuple[str, str, float]] = ambiguity_info.get('candidates') or []
    matches = [
        (pid, name, score)
        for pid, name, score in cands
        if _dob_close(dob, canonical_dob.get(f'fb_{pid}'))
    ]
    if len(matches) == 1:
        pid, _name, score = matches[0]
        return f'fb_{pid}', 'name_team_dob', float(score)
    return None


# ---------------------------------------------------------------------------
# Trino plumbing
# ---------------------------------------------------------------------------
def _get_trino_connection(
    catalog: str = 'iceberg',
) -> 'trino_lib.dbapi.Connection':
    """Mirrors :func:`utils.silver_tasks._get_trino_connection`.

    Duplicated rather than imported because :mod:`silver_tasks` carries
    ~700 lines of Silver-specific logic; the duplication is small (40 lines)
    and keeps this module self-contained for unit testing.
    """
    host = os.environ.get('TRINO_HOST', 'trino')
    user = os.environ.get('TRINO_USER', 'airflow')
    password = os.environ.get('TRINO_PASSWORD')

    if password:
        port = int(os.environ.get('TRINO_PORT', 8443))
        return trino_lib.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            http_scheme='https',
            auth=trino_lib.auth.BasicAuthentication(user, password),
            verify=False,  # nosec B501 — self-signed Trino cert; см. configs/trino/config.properties
        )

    port = int(os.environ.get('TRINO_PORT', 8080))
    logger.info("TRINO_PASSWORD not set, connecting via HTTP (no auth)")
    return trino_lib.dbapi.connect(
        host=host,
        port=port,
        user=user,
        catalog=catalog,
    )


def _execute(conn, sql: str, fetch: bool = False):
    """Execute a single SQL statement and consume results.

    Critical: *every* DDL/DML must call ``fetchall()``, otherwise Trino
    treats the missing fetch as a client-side cancellation and the next
    query returns USER_CANCELED. See CLAUDE.md / data-platform memory.
    """
    cur = conn.cursor()
    try:
        cur.execute(sql)
        if fetch:
            return cur.fetchall()
        try:
            cur.fetchall()
        except Exception:
            pass
        return None
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Bronze readers
# ---------------------------------------------------------------------------
# NOTE on column names: query layout is taken verbatim from
# scripts/r2_resolver_proto.py which was successfully run on Bronze APL
# 2024-25 (see docs/research/R2_player_resolver.md). Bronze schemas:
#   * fbref_player_stats     -> player_id, player, squad, season(int), league
#   * understat_players      -> player_id, player, team, season(varchar), league
#   * whoscored_events       -> player_id, player, team, season(varchar), league
# (squad/team naming difference is REAL — FBref calls it "squad").
# Re-discovered column names would be a regression so they are pinned here.


def _seasons_in_clause(seasons: List[Any]) -> str:
    """Render a list of season values as a Trino IN clause body.

    Mixes int and string values verbatim — caller is responsible for type:
    FBref Bronze stores season as bigint; Understat/WhoScored store it as
    varchar (e.g. '2425'). Trino will type-coerce inside IN if needed.
    """
    parts: List[str] = []
    for s in seasons:
        if isinstance(s, int):
            parts.append(str(s))
        else:
            # Whitelist literal — season strings are 4-digit slugs ('2425')
            # or alphanumeric. Refuse anything else outright (defense in depth
            # — quotes, comments, statement separators all get blocked here).
            ss = str(s)
            if not re.fullmatch(r"[A-Za-z0-9_]+", ss):
                raise ValueError(f"unsafe season literal: {ss!r}")
            parts.append(f"'{ss}'")
    return ', '.join(parts)


def _fetch_fbref_players(
    conn, league: str, fbref_seasons: List[int]
) -> List[Dict[str, Any]]:
    sql = f"""
        SELECT player_id, player, squad, league, CAST(season AS varchar) AS season
        FROM iceberg.bronze.fbref_player_stats
        WHERE league = '{_sql_escape(league)}'
          AND season IN ({_seasons_in_clause(fbref_seasons)})
          AND stat_type = 'stats'
          AND player_id IS NOT NULL
        GROUP BY player_id, player, squad, league, CAST(season AS varchar)
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for pid, name, squad, lg, season in rows:
        season_slug = _fbref_year_to_slug(season)
        # Dedup by (pid, squad, season): mid-season transfers (e.g. Palmer
        # Man City->Chelsea in 2023-24) produce two FBref rows in the same
        # season — keep both so spine carries the player in both team
        # buckets. Without squad in the key Understat candidates seeking
        # the post-transfer club get false orphan'd (broke Cole Palmer
        # known-pair regression).
        key = (str(pid), squad, season_slug)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                'source': 'fbref',
                'player_id': str(pid),
                'source_id': str(pid),
                'player_name': name,
                'raw_team_name': squad,
                'canonical_team': canonical_team_for_resolver(squad, 'fbref'),
                'league': lg,
                'season': season_slug,
                # FBref is the canonical spine; one canonical_id never has
                # multiple FBref source_ids per (league, season) so the
                # dedup tiebreaker never fires. Sentinel kept for uniformity.
                'bronze_signal': -1.0,
            }
        )
    return out


def _fetch_understat_players(
    conn, league: str, source_seasons: List[str]
) -> List[Dict[str, Any]]:
    # ``minutes`` is a season-level column on bronze.understat_players; used
    # as the dedup tiebreaker in _dedup_canonical_per_season (issue #70) so
    # the row tied to the player's primary club wins when one canonical_id
    # maps to multiple Understat source_ids (Harrison Reed 910/6827).
    sql = f"""
        SELECT CAST(player_id AS varchar) AS pid,
               player,
               team,
               league,
               season,
               CAST(COALESCE(MAX(minutes), 0) AS DOUBLE) AS bronze_signal
        FROM iceberg.bronze.understat_players
        WHERE league = '{_sql_escape(league)}'
          AND season IN ({_seasons_in_clause(source_seasons)})
          AND player IS NOT NULL
        GROUP BY player_id, player, team, league, season
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for pid, name, team, lg, season, signal in rows:
        # Dedup by (pid, team, season): same player_id may legitimately
        # appear in multiple seasons (and across teams within a season for
        # mid-season transfers). Without (team, season) in the key, the
        # first encountered row wins and every other (season, team) row
        # for this player_id is dropped — orphaning Understat anchors for
        # all non-first seasons. Bug introduced 2026-05-09 (E1), parity
        # fix for FBref shipped same day but never applied to Understat.
        key = (pid, team, season)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                'source': 'understat',
                'source_id': pid,
                'player_name': name,
                'raw_team_name': team,
                'canonical_team': canonical_team_for_resolver(team, 'understat'),
                'league': lg,
                'season': season,
                'bronze_signal': float(signal) if signal is not None else 0.0,
            }
        )
    return out


def _fetch_whoscored_players(
    conn, league: str, source_seasons: List[str]
) -> List[Dict[str, Any]]:
    # COUNT(DISTINCT game_id) is the games-played proxy used as the dedup
    # tiebreaker in _dedup_canonical_per_season (issue #70). WhoScored is
    # event-grain; no native minutes column on bronze.whoscored_events.
    sql = f"""
        SELECT CAST(CAST(player_id AS bigint) AS varchar) AS pid,
               MAX(player) AS player,
               MAX(team) AS team,
               league,
               season,
               CAST(COUNT(DISTINCT CAST(game_id AS bigint)) AS DOUBLE) AS bronze_signal
        FROM iceberg.bronze.whoscored_events
        WHERE league = '{_sql_escape(league)}'
          AND season IN ({_seasons_in_clause(source_seasons)})
          AND player_id IS NOT NULL
          AND player IS NOT NULL
        GROUP BY CAST(player_id AS bigint), league, season
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    for pid, name, team, lg, season, signal in rows:
        out.append(
            {
                'source': 'whoscored',
                'source_id': pid,
                'player_name': name,
                'raw_team_name': team,
                'canonical_team': canonical_team_for_resolver(team, 'whoscored'),
                'league': lg,
                'season': season,
                'bronze_signal': float(signal) if signal is not None else 0.0,
            }
        )
    return out


def _fetch_fotmob_players(
    conn, league: str, fbref_seasons: List[int]
) -> List[Dict[str, Any]]:
    """Read FotMob player anchor rows for the resolver cascade.

    Identity (player_id + name + team) comes from ``silver.fotmob_lineup``
    (per-match lineups parsed from bronze.fotmob_match_details.lineup_json),
    NOT from ``bronze.fotmob_player_details``. Rationale (#825): the
    player_details scraper seeds player ids from the *current* team squads
    (``_get_team_data`` takes no season arg), so its historical partitions
    carry today's squad members rather than the players who actually appeared
    that season — only ~41/620 anchors survived for 2016/17. fotmob_lineup is
    season-accurate (team_name = the team the player lined up for in that
    match), giving a full anchor per real season participant across all 10
    seasons. primary_team = the team with the most appearances that season
    (max_by) so a mid-season transfer maps to its dominant club.

    The minutes-played signal still comes from ``bronze.fotmob_player_stats``
    (participant_id == fotmob player id, so the join matches the lineup
    player_id historically too). Used for the senior-appearance signal<=0
    filter (#563) and the _dedup_canonical_per_season tiebreaker (#70).

    fotmob_lineup.season is already a slug ('2526'); convert the incoming
    FBref-style year list to slugs for the filter. player_stats season is a
    bigint year-of-start → fold to a slug in-query so the minutes join aligns.
    """
    season_slugs = [_fbref_year_to_slug(y) for y in fbref_seasons]
    sql = f"""
        WITH stats_slug AS (
            SELECT
                CAST(participant_id AS VARCHAR) AS player_id,
                league,
                LPAD(CAST(MOD(season,     100) AS varchar), 2, '0')
                    || LPAD(CAST(MOD(season + 1, 100) AS varchar), 2, '0')
                    AS season,
                minutes_played
            FROM iceberg.bronze.fotmob_player_stats
            WHERE league = '{_sql_escape(league)}'
              AND season IN ({_seasons_in_clause(fbref_seasons)})
        ),
        mins AS (
            SELECT player_id, league, season,
                   MAX(minutes_played) AS minutes_played
            FROM stats_slug
            GROUP BY player_id, league, season
        ),
        per_team AS (
            SELECT
                player_id,
                league,
                season,
                team_name,
                arbitrary(player_name) AS player_name,
                COUNT(*) AS apps
            FROM iceberg.silver.fotmob_lineup
            WHERE league = '{_sql_escape(league)}'
              AND season IN ({_seasons_in_clause(season_slugs)})
              AND player_id IS NOT NULL
              AND player_name IS NOT NULL
            GROUP BY player_id, league, season, team_name
        ),
        anchors AS (
            SELECT
                player_id,
                max_by(player_name, apps) AS name,
                max_by(team_name, apps)   AS primary_team_name,
                league,
                season
            FROM per_team
            GROUP BY player_id, league, season
        )
        SELECT a.player_id,
               a.name,
               a.primary_team_name,
               a.league,
               a.season,
               COALESCE(m.minutes_played, 0.0) AS bronze_signal
        FROM anchors a
        LEFT JOIN mins m
          ON m.player_id = a.player_id
         AND m.league    = a.league
         AND m.season    = a.season
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for pid, name, team, lg, season, signal in rows:
        # Align FotMob population with FBref coverage (issue #563). FBref lists
        # only players with senior APL appearances and never carries U21 squads,
        # so youth teams and zero-minute deep-squad/reserve players are
        # structural non-overlaps, not resolver misses — they inflate the orphan
        # rate ~10pp. Mirrors the Capology active+loan filter (_fetch_capology).
        if _is_youth_team(team) or float(signal or 0.0) <= 0.0:
            continue
        # Dedup by (pid, team, season). silver.fotmob_lineup season is already a
        # slug, and `anchors` collapses to one row per (player_id, season), so
        # this is a belt-and-braces guard; multi-season players keep separate
        # xref rows.
        key = (str(pid), team, season)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                'source': 'fotmob',
                'source_id': str(pid),
                'player_name': name,
                'raw_team_name': team,
                'canonical_team': canonical_team_for_resolver(team, 'fotmob'),
                'league': lg,
                'season': season,
                'bronze_signal': float(signal) if signal is not None else 0.0,
            }
        )
    return out


def _fetch_sofascore_players(
    conn, league: str, source_seasons: List[str]
) -> List[Dict[str, Any]]:
    """Read SofaScore player anchor rows for the resolver cascade.

    Bronze ``sofascore_player_season_stats`` does NOT carry a player_name
    column (the season-stats flattener only emits team_name + IDs + stats).
    The display name needed for the fuzzy ``name_team`` tier comes from a
    LEFT JOIN to ``bronze.sofascore_player_profile`` which DOES carry
    ``name`` / ``short_name``. If profile is sparse, the cascade will
    silently fall through to orphan — that is expected.
    """
    # Minutes-played proxy via SUM over bronze.sofascore_event_player_stats
    # (per-match grain). Used as dedup tiebreaker in
    # _dedup_canonical_per_season (issue #70). NULL fallback to 0.0.
    sql = f"""
        WITH mins AS (
            SELECT
                player_id,
                league,
                season,
                SUM(TRY_CAST(minutes_played AS DOUBLE)) AS minutes_played
            FROM iceberg.bronze.sofascore_event_player_stats
            WHERE league = '{_sql_escape(league)}'
              AND season IN ({_seasons_in_clause(source_seasons)})
              AND player_id IS NOT NULL
            GROUP BY player_id, league, season
        )
        SELECT
            CAST(b.player_id AS varchar) AS pid,
            COALESCE(MAX(p.name), MAX(p.short_name)) AS player_name,
            MAX(b.team_name) AS team,
            b.league,
            b.season,
            COALESCE(MAX(m.minutes_played), 0.0) AS bronze_signal
        FROM iceberg.bronze.sofascore_player_season_stats b
        LEFT JOIN iceberg.bronze.sofascore_player_profile p
          ON p.player_id = b.player_id
        LEFT JOIN mins m
          ON m.player_id = b.player_id
         AND m.league = b.league
         AND m.season = b.season
        WHERE b.league = '{_sql_escape(league)}'
          AND b.season IN ({_seasons_in_clause(source_seasons)})
          AND b.player_id IS NOT NULL
        GROUP BY CAST(b.player_id AS varchar), b.league, b.season
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    for pid, name, team, lg, season, signal in rows:
        out.append(
            {
                'source': 'sofascore',
                'source_id': pid,
                'player_name': name,
                'raw_team_name': team,
                'canonical_team': canonical_team_for_resolver(team, 'sofascore'),
                'league': lg,
                'season': season,
                'bronze_signal': float(signal) if signal is not None else 0.0,
            }
        )
    return out


def _fetch_transfermarkt_players(
    conn, league: str, source_seasons: List[str]
) -> List[Dict[str, Any]]:
    """Read Transfermarkt player anchor rows for the resolver cascade.

    Bronze ``transfermarkt_players`` is a per-season snapshot (one row per
    (player_id, league, season)) with rich attributes — for the resolver
    we only need name + current_club_name + season; the rest is consumed
    by the downstream Silver CTAS (#60).

    Season is stored as the 4-digit slug ('2526') matching Understat /
    WhoScored / SofaScore conventions, so no slug conversion is needed.
    """
    sql = f"""
        SELECT
            player_id,
            name,
            current_club_name,
            league,
            season
        FROM iceberg.bronze.transfermarkt_players
        WHERE league = '{_sql_escape(league)}'
          AND season IN ({_seasons_in_clause(source_seasons)})
          AND player_id IS NOT NULL
          AND name IS NOT NULL
        GROUP BY player_id, name, current_club_name, league, season
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for pid, name, team, lg, season in rows:
        key = (str(pid), team, season)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                'source': 'transfermarkt',
                'source_id': str(pid),
                'player_name': name,
                'raw_team_name': team,
                'canonical_team': canonical_team_for_resolver(team, 'transfermarkt'),
                'league': lg,
                'season': season,
                # No minutes/games proxy on Transfermarkt Bronze; dedup
                # tiebreaker degenerates to source_id ordering for any
                # canonical collisions (none observed as of issue #70).
                'bronze_signal': -1.0,
            }
        )
    return out


def _fetch_capology_players(
    conn, league: str, source_seasons: List[str]
) -> List[Dict[str, Any]]:
    """Read Capology salary-snapshot anchors for the resolver cascade.

    Bronze ``capology_player_salaries`` is keyed by player_slug + club_slug
    + season + currency; we filter to a single currency to avoid 3× row
    inflation when EUR/USD partitions land (MVP: GBP only).

    Multi-club edge case: a player who changed clubs mid-season has TWO
    rows (one per club). The fuzzy-match tier uses (season, canonical_team)
    buckets so each row still tries to match its FBref counterpart inside
    its own club bucket — both rows will resolve to the same canonical_id
    on success, and the (source, source_id, league, season) dedup in
    ``xref_player`` PK collapses them.
    """
    # Filter to roster-active or on-loan players. Bronze carries ~28% of
    # rows with status='Inactive' (released, youth, academy) — these have
    # no FBref / Understat / WhoScored counterpart and would silently
    # inflate the orphan rate by structural ~30pp. Active+loan keeps the
    # set comparable to the other 6 sources' rostered-only output.
    sql = f"""
        SELECT
            player_slug,
            MAX(player_name) AS player_name,
            MAX(club_name)   AS club_name,
            league,
            season
        FROM iceberg.bronze.capology_player_salaries
        WHERE league = '{_sql_escape(league)}'
          AND season IN ({_seasons_in_clause(source_seasons)})
          AND currency = 'GBP'
          AND player_slug IS NOT NULL
          AND player_name IS NOT NULL
          AND (active = true OR loan = true)
        GROUP BY player_slug, league, season
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    for slug, name, team, lg, season in rows:
        out.append(
            {
                'source': 'capology',
                'source_id': slug,
                'player_name': name,
                'raw_team_name': team,
                'canonical_team': canonical_team_for_resolver(team, 'capology'),
                'league': lg,
                'season': season,
                # No minutes proxy on Capology Bronze; dedup tiebreaker
                # degenerates to source_id ordering.
                'bronze_signal': -1.0,
            }
        )
    return out


def _slug_to_sofifa_edition_num(slug: str) -> str:
    """Map a football-season slug to a SoFIFA edition number.

    SoFIFA / EA FC editions are named after the season's *end* year: EA FC 26
    ships in Sep 2025 and covers the 2025/26 season. So slug ``'2526'`` ->
    edition ``'26'``, ``'2425'`` -> ``'25'``. We compare on the numeric suffix
    only so the ``'FIFA'`` -> ``'FC'`` rename (FC 24+) is transparent.
    """
    s = str(slug)
    return s[2:4]


def _fetch_sofifa_players(
    conn, league: str, source_seasons: List[str]
) -> List[Dict[str, Any]]:
    """Read SoFIFA player anchor rows for the resolver cascade.

    Bronze ``sofifa_players`` is a per-edition snapshot keyed by
    ``(player_id, fifa_edition)`` carrying only identity columns (name / team /
    league); the FIFA attribute ratings live in a separate Bronze table and are
    consumed by the downstream Silver CTAS, not the resolver.

    ``fifa_edition`` is stored as the marketing name (``'FC 26'``), so we map
    each requested football-season slug to its edition number and filter on the
    numeric suffix. The emitted ``season`` is the football-season slug (not the
    edition) so ``xref_player.season`` lines up with every other source and the
    Silver JOIN ``xp.season = b.season`` works after the same mapping.
    """
    # edition_num ('26') -> football slug ('2526') for the requested seasons.
    edition_to_slug = {
        _slug_to_sofifa_edition_num(s): str(s) for s in source_seasons
    }
    wanted_editions = [e for e in edition_to_slug if e]
    if not wanted_editions:
        return []

    sql = f"""
        SELECT
            player_id,
            player,
            team,
            league,
            fifa_edition
        FROM iceberg.bronze.sofifa_players
        WHERE league = '{_sql_escape(league)}'
          AND regexp_extract(fifa_edition, '(\\d+)', 1)
              IN ({_seasons_in_clause(wanted_editions)})
          AND player_id IS NOT NULL
          AND player IS NOT NULL
        GROUP BY player_id, player, team, league, fifa_edition
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for pid, name, team, lg, edition in rows:
        # Extract the numeric suffix of the edition string ('FC 26' -> '26').
        digits = ''.join(ch for ch in str(edition) if ch.isdigit())
        edition_num = digits[-2:] if len(digits) >= 2 else digits
        season = edition_to_slug.get(edition_num)
        if season is None:
            continue
        key = (str(pid), team, season)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                'source': 'sofifa',
                'source_id': str(pid),
                'player_name': name,
                'raw_team_name': team,
                'canonical_team': canonical_team_for_resolver(team, 'sofifa'),
                'league': lg,
                'season': season,
                # No minutes/games proxy on SoFIFA Bronze; dedup tiebreaker
                # degenerates to source_id ordering for canonical collisions.
                'bronze_signal': -1.0,
            }
        )
    return out


def _fetch_espn_players(
    conn, league: str, source_seasons: List[str]
) -> List[Dict[str, Any]]:
    """Read ESPN player anchor rows for the resolver cascade (#692).

    ESPN's matchsheet lineups (``bronze.espn_lineup``) carry NO native
    player_id — the only identity columns are the player display ``name`` and
    the ``team``. We therefore synthesise ``source_id = '<player>|<team>'`` so
    the ``xref_player`` PK ``(source, source_id, league, season)`` stays unique
    even for namesakes on different clubs (mirrors the silver.espn_lineup dedup
    grain of (match_id, team, player)). The downstream gold/fct_lineup JOIN
    keys on (display_name, raw_team_name, league, season), NOT on this
    composite, so the delimiter choice is internal to the resolver.

    Season is the 4-digit slug ('2526') exactly as Understat/WhoScored store
    it, so no slug conversion is needed. There is no minutes/games proxy on
    ESPN lineups, so ``bronze_signal`` degenerates to the -1.0 sentinel (like
    Transfermarkt / Capology / SoFIFA).
    """
    sql = f"""
        SELECT
            player,
            team,
            league,
            CAST(season AS varchar) AS season
        FROM iceberg.bronze.espn_lineup
        WHERE league = '{_sql_escape(league)}'
          AND season IN ({_seasons_in_clause(source_seasons)})
          AND player IS NOT NULL
          AND team IS NOT NULL
        GROUP BY player, team, league, CAST(season AS varchar)
    """
    rows = _execute(conn, sql, fetch=True) or []
    out: List[Dict[str, Any]] = []
    for name, team, lg, season in rows:
        out.append(
            {
                'source': 'espn',
                # Composite identity — ESPN has no native player_id.
                'source_id': f"{name}|{team}",
                'player_name': name,
                'raw_team_name': team,
                'canonical_team': canonical_team_for_resolver(team, 'espn'),
                'league': lg,
                'season': season,
                # No minutes/games proxy on ESPN lineups; dedup tiebreaker
                # degenerates to source_id ordering for canonical collisions.
                'bronze_signal': -1.0,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Bronze DOB readers (cross-source corroboration, tier name_team_dob)
# ---------------------------------------------------------------------------
# DOB is a time-invariant attribute → each map is keyed by source_id only
# (no season), taking the freshest Bronze value via max_by(..., _ingested_at).
# Every reader degrades to an empty map on ANY error (missing table, absent
# column) — the DOB feature is strictly additive and must never fail a run.
# Bronze only: silver profile tables JOIN xref_player themselves, so reading
# them here would create a wrong-link feedback loop (see module docstring).


def _fetch_dob_map(conn, sql: str, source: str) -> Dict[str, Any]:
    """Run one DOB projection; ``{source_id: date}`` or empty map on error."""
    try:
        rows = _execute(conn, sql, fetch=True) or []
    except Exception as e:
        logger.warning(
            "DOB fetch for %s failed (%s) — corroboration disabled for "
            "this source this run.", source, e,
        )
        return {}
    return {str(pid): dob for pid, dob in rows if pid is not None and dob is not None}


def _fetch_dob_maps(
    conn, league: str, source_seasons: List[str]
) -> Dict[str, Dict[str, Any]]:
    """DOB maps for the 5 DOB-carrying sources.

    Returns ``{source: {source_id: datetime.date}}``. Understat / Capology /
    ESPN carry no DOB in Bronze; FBref (the spine) is not scraped for DOB —
    hence corroboration between non-FBref sources (module docstring).
    """
    lg = _sql_escape(league)
    seasons = _seasons_in_clause(source_seasons)
    queries = {
        # date_of_birth is a varchar passthrough (ISO) — TRY_CAST like
        # gold/dim_player.sql.j2 does.
        'fotmob': f"""
            SELECT CAST(player_id AS varchar),
                   max_by(TRY_CAST(date_of_birth AS DATE), _ingested_at)
            FROM iceberg.bronze.fotmob_team_squad
            WHERE league = '{lg}' AND player_id IS NOT NULL
            GROUP BY CAST(player_id AS varchar)
        """,
        'sofascore': f"""
            SELECT CAST(player_id AS varchar),
                   max_by(TRY_CAST(date_of_birth AS DATE), _ingested_at)
            FROM iceberg.bronze.sofascore_player_profile
            WHERE player_id IS NOT NULL
            GROUP BY CAST(player_id AS varchar)
        """,
        'transfermarkt': f"""
            SELECT CAST(player_id AS varchar), max_by(dob, _ingested_at)
            FROM iceberg.bronze.transfermarkt_players
            WHERE league = '{lg}' AND season IN ({seasons})
              AND player_id IS NOT NULL
            GROUP BY CAST(player_id AS varchar)
        """,
        # SoFIFA dob is 'Mon D, YYYY' ('Nov 9, 1982') — date_parse pattern
        # proven in gold/dim_player.sql.j2 (#584). No league column filter:
        # ratings are keyed (player_id, fifa_edition) and the map is only
        # consulted for anchors already scoped to this league.
        'sofifa': f"""
            SELECT CAST(player_id AS varchar),
                   max_by(TRY(CAST(date_parse(dob, '%b %e, %Y') AS DATE)),
                          _ingested_at)
            FROM iceberg.bronze.sofifa_player_ratings
            WHERE player_id IS NOT NULL AND dob IS NOT NULL
            GROUP BY CAST(player_id AS varchar)
        """,
        # whoscored_player_profile.date_of_birth is normalised to ISO by the
        # scraper. player_id cast mirrors _fetch_whoscored_players so map
        # keys line up with anchor source_ids.
        'whoscored': f"""
            SELECT CAST(CAST(player_id AS bigint) AS varchar),
                   max_by(TRY_CAST(date_of_birth AS DATE), _ingested_at)
            FROM iceberg.bronze.whoscored_player_profile
            WHERE player_id IS NOT NULL
            GROUP BY CAST(CAST(player_id AS bigint) AS varchar)
        """,
    }
    maps = {src: _fetch_dob_map(conn, sql, src) for src, sql in queries.items()}
    logger.info(
        "DOB maps: %s",
        {src: len(m) for src, m in maps.items()},
    )
    return maps


# ---------------------------------------------------------------------------
# Trino write helpers
# ---------------------------------------------------------------------------
def _sql_escape(s: str) -> str:
    """Escape an apostrophe inside a single-quoted SQL literal."""
    return s.replace("'", "''")


def _sql_str(s: Optional[str]) -> str:
    if s is None:
        return 'NULL'
    return f"'{_sql_escape(str(s))}'"


def _sql_double(v: Optional[float]) -> str:
    if v is None:
        return 'NULL'
    return f"CAST({float(v)} AS DOUBLE)"


def _value_tuple(row: Dict[str, Any]) -> str:
    return (
        '('
        f"{_sql_str(row['canonical_id'])}, "
        f"{_sql_str(row['source'])}, "
        f"{_sql_str(row['source_id'])}, "
        f"{_sql_str(row['display_name'])}, "
        f"{_sql_str(row['league'])}, "
        f"{_sql_str(row['season'])}, "
        f"{_sql_str(row['confidence'])}, "
        f"{_sql_double(row['match_score'])}, "
        f"{_sql_str(row['raw_team_name'])}, "
        f"{_sql_str(row['canonical_team'])}, "
        # Silver lineage column (charter §4). xref_player is Python-materialised,
        # so the CTAS runner does not inject this — the resolver adds it here.
        'CURRENT_TIMESTAMP'
        ')'
    )


def _create_target_table(conn, target_table: str) -> None:
    """DROP + CREATE the Iceberg target. Partitioned by (league, season).

    A no-op ``SELECT COUNT(*)`` is appended after CREATE — without it Trino's
    in-session HMS cache occasionally yields ``Table UUID does not match``
    (cache holds the pre-DROP UUID) or ``Table not found`` (cache hasn't
    seen the CREATE yet) on the immediately-following INSERT. The COUNT(*)
    forces the session to re-bind the table → fresh UUID resolves cleanly.
    """
    _execute(conn, f"DROP TABLE IF EXISTS {target_table}")
    _execute(
        conn,
        f"""
        CREATE TABLE {target_table} (
            canonical_id   varchar,
            source         varchar,
            source_id      varchar,
            display_name   varchar,
            league         varchar,
            season         varchar,
            confidence     varchar,
            match_score    double,
            raw_team_name  varchar,
            canonical_team varchar,
            _silver_created_at timestamp(6) with time zone
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['league', 'season']
        )
        """,
    )
    _execute(conn, f"SELECT COUNT(*) FROM {target_table}", fetch=True)


def _create_review_table(conn, review_table: str) -> None:
    """DROP + CREATE the Iceberg ``xref_player_review`` table.

    Schema mirrors the Fellegi-Sunter clerical-review band: each row carries
    the source's identifying tuple plus the candidate set (FBref ids and
    display names) and the rule label that flagged the ambiguity. Reviewers
    consume this through Superset / BI to disambiguate manually before
    eventual promotion into ``xref_player`` (or to extend ``player_aliases.yaml``
    so the next resolver run auto-resolves the case).
    """
    _execute(conn, f"DROP TABLE IF EXISTS {review_table}")
    _execute(
        conn,
        f"""
        CREATE TABLE {review_table} (
            source           varchar,
            source_id        varchar,
            display_name     varchar,
            raw_team_name    varchar,
            canonical_team   varchar,
            league           varchar,
            season           varchar,
            candidates       array(varchar),
            candidate_names  array(varchar),
            rule             varchar,
            score            double,
            detected_at      timestamp(6) with time zone
        )
        WITH (
            format = 'PARQUET',
            partitioning = ARRAY['league', 'season']
        )
        """,
    )
    # Force HMS cache refresh — see _create_target_table for rationale.
    _execute(conn, f"SELECT COUNT(*) FROM {review_table}", fetch=True)


def _insert_rows(
    conn,
    target_table: str,
    rows: List[Dict[str, Any]],
    chunk_size: int,
) -> int:
    """Batched ``INSERT INTO ... VALUES (...), (...)``.

    Returns rows actually inserted.
    """
    written = 0
    cols = (
        'canonical_id, source, source_id, display_name, league, season, '
        'confidence, match_score, raw_team_name, canonical_team, '
        '_silver_created_at'
    )
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        values_sql = ',\n'.join(_value_tuple(r) for r in chunk)
        _execute(
            conn,
            f"INSERT INTO {target_table} ({cols}) VALUES {values_sql}",
        )
        written += len(chunk)
        logger.info(
            "  wrote %d/%d rows",
            min(i + chunk_size, len(rows)),
            len(rows),
        )
    return written


def _sql_array(values: List[str]) -> str:
    """Render a Python list-of-str as a Trino ARRAY literal.

    Empty input yields ``CAST(ARRAY[] AS array(varchar))`` so Iceberg can
    infer the element type even when no values are present (avoids
    ``ARRAY[]`` of element-type unknown which Trino rejects).
    """
    if not values:
        return "CAST(ARRAY[] AS array(varchar))"
    parts = [_sql_str(v) for v in values]
    return f"ARRAY[{', '.join(parts)}]"


def _sql_timestamp(ts: Any) -> str:
    """Render a datetime / iso-string as a Trino ``TIMESTAMP(6) WITH TIME ZONE``.

    Accepts ``datetime`` (uses isoformat), ``str`` (assumed ISO-8601 already
    safe), or ``None`` → falls back to ``CURRENT_TIMESTAMP`` so reviewers
    always have a non-null detection timestamp.
    """
    if ts is None:
        return "CURRENT_TIMESTAMP"
    if hasattr(ts, 'isoformat'):
        return f"from_iso8601_timestamp('{ts.isoformat()}')"
    return f"from_iso8601_timestamp('{_sql_escape(str(ts))}')"


def _value_tuple_review(row: Dict[str, Any]) -> str:
    return (
        '('
        f"{_sql_str(row['source'])}, "
        f"{_sql_str(row['source_id'])}, "
        f"{_sql_str(row['display_name'])}, "
        f"{_sql_str(row['raw_team_name'])}, "
        f"{_sql_str(row['canonical_team'])}, "
        f"{_sql_str(row['league'])}, "
        f"{_sql_str(row['season'])}, "
        f"{_sql_array(row.get('candidates') or [])}, "
        f"{_sql_array(row.get('candidate_names') or [])}, "
        f"{_sql_str(row['rule'])}, "
        f"{_sql_double(row.get('score'))}, "
        f"{_sql_timestamp(row.get('detected_at'))}"
        ')'
    )


def _insert_review_rows(
    conn,
    review_table: str,
    rows: List[Dict[str, Any]],
    chunk_size: int,
) -> int:
    """Batched insert into ``xref_player_review``. Returns rows written."""
    if not rows:
        return 0
    written = 0
    cols = (
        'source, source_id, display_name, raw_team_name, canonical_team, '
        'league, season, candidates, candidate_names, rule, score, detected_at'
    )
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        values_sql = ',\n'.join(_value_tuple_review(r) for r in chunk)
        _execute(
            conn,
            f"INSERT INTO {review_table} ({cols}) VALUES {values_sql}",
        )
        written += len(chunk)
        logger.info(
            "  wrote %d/%d review rows",
            min(i + chunk_size, len(rows)),
            len(rows),
        )
    return written


# ---------------------------------------------------------------------------
# Build the materialised rows
# ---------------------------------------------------------------------------
def _build_review_row(
    candidate: Dict[str, Any],
    ambiguity_info: Dict[str, Any],
    detected_at: Any,
) -> Dict[str, Any]:
    """Shape a Fellegi-Sunter clerical-review row for ``xref_player_review``.

    ``ambiguity_info`` is the dict mutated by ``cascade_resolve`` when a
    tier returned ambiguous. The wire-format here is the SQL row that
    ``_insert_review_rows`` will eventually emit.
    """
    cands: List[Tuple[str, str, float]] = ambiguity_info.get('candidates', []) or []
    return {
        'source': candidate['source'],
        'source_id': str(candidate['source_id']),
        'display_name': candidate.get('player_name'),
        'raw_team_name': candidate.get('raw_team_name'),
        'canonical_team': candidate.get('canonical_team'),
        'league': candidate.get('league'),
        'season': candidate.get('season'),
        'candidates': [str(pid) for pid, _name, _score in cands],
        'candidate_names': [str(_name) for _pid, _name, _score in cands],
        'rule': str(ambiguity_info.get('rule', 'unknown')),
        'score': float(ambiguity_info.get('best_score', 0.0) or 0.0),
        'detected_at': detected_at,
    }


def _resolve_all(
    fb_rows: List[Dict[str, Any]],
    us_rows: List[Dict[str, Any]],
    ws_rows: List[Dict[str, Any]],
    ss_rows: List[Dict[str, Any]],
    fm_rows: Optional[List[Dict[str, Any]]] = None,
    tm_rows: Optional[List[Dict[str, Any]]] = None,
    cap_rows: Optional[List[Dict[str, Any]]] = None,
    sf_rows: Optional[List[Dict[str, Any]]] = None,
    es_rows: Optional[List[Dict[str, Any]]] = None,
    *,
    nn: Any = None,
    detected_at: Any = None,
    dob_stats_out: Optional[Dict[str, Any]] = None,
) -> Tuple[
    List[Dict[str, Any]],
    List[Dict[str, Any]],
    Dict[str, Dict[str, int]],
]:
    """Apply the cascade across all non-FBref sources, then DOB-adjudicate.

    Three phases (the cascade itself, :func:`cascade_resolve`, is untouched):

    1. Cascade every candidate as before, but DEFER ambiguous rows instead
       of emitting review rows immediately.
    2. Consolidate a per-canonical DOB map from resolved anchor-tier rows
       (:func:`build_canonical_dob_map`); rows lacking DOB contribute
       nothing, so with no DOB anywhere the output is byte-identical to the
       pre-DOB behaviour.
    3. (a) VETO: a fuzzy-resolved row (:data:`DOB_VETO_TIERS`) whose DOB
       contradicts the canonical's consolidated DOB moves to the review
       queue with ``rule='dob_veto'`` (a wrong link is worse than a missing
       one — a human or a ``player_aliases.yaml`` entry decides).
       (b) ADJUDICATE: a deferred ambiguous row whose DOB uniquely matches
       one candidate is promoted with ``confidence='name_team_dob'``;
       otherwise it lands in review exactly as before.

    Returns ``(xref_rows, review_rows, per_source_stats)``.

    Args:
        dob_stats_out: optional dict mutated in place with DOB-phase counters
            (``canonical_dob_map``, ``dob_conflicts``, ``conflicts`` list,
            ``promoted_from_review``, ``vetoed``) — mirrors the
            ``ambiguity_out`` style so the 3-tuple return contract stays
            frozen for the T4 DAG integration.

    ``per_source_stats`` shape::

        {'fbref':     {'total': N, 'resolved': N, 'orphan': 0,  'ambiguous': 0},
         'understat': {'total': N, 'resolved': X, 'orphan': Y,  'ambiguous': Z},
         'whoscored': {'total': N, 'resolved': X, 'orphan': Y,  'ambiguous': Z},
         'fotmob':    {'total': N, 'resolved': X, 'orphan': Y,  'ambiguous': Z},
         'sofascore': {'total': N, 'resolved': X, 'orphan': Y,  'ambiguous': Z}}

    NB: ``total = resolved + orphan + ambiguous``. Ambiguous candidates are
    NOT inserted into ``xref_player`` — they live in ``xref_player_review``
    until a human reviewer (or a future tier-rule extension) disambiguates.

    Args:
        nn: optional ``nicknames.NickNamer`` instance used by tier 2.7.
        detected_at: timestamp written into ``xref_player_review.detected_at``.
            Defaults to ``time.time()`` rendered as ``datetime`` at write
            time inside :func:`_insert_review_rows` if left as ``None``.
    """
    spine = _FBrefSpine(fb_rows)
    out: List[Dict[str, Any]] = []
    review: List[Dict[str, Any]] = []
    stats: Dict[str, Dict[str, int]] = {
        s: {'total': 0, 'resolved': 0, 'orphan': 0, 'ambiguous': 0}
        for s in SOURCES
    }

    # FBref spine: every FBref player IS canonical.
    #
    # Dedup by ``(player_id, season)`` BEFORE emission — the spine intentionally
    # keeps multiple ``(player_id, squad, season)`` rows for mid-season transfers
    # so the bucket index reaches both clubs (Cole Palmer Man-City→Chelsea), but
    # ``xref_player``'s PK is ``(source, source_id, league, season)``. Without
    # this dedup a transferred player produces 2 rows with the same PK and the
    # downstream DQ ``no_duplicates`` check fails. The "lost" information is
    # the secondary canonical_team for the season — which is recoverable from
    # ``bronze.fbref_player_stats`` directly when needed.
    seen_fb_keys: set = set()
    for row in fb_rows:
        key = (row['player_id'], row['season'])
        if key in seen_fb_keys:
            continue
        seen_fb_keys.add(key)
        out.append(
            {
                'canonical_id': f"fb_{row['player_id']}",
                'source': 'fbref',
                'source_id': row['source_id'],
                'display_name': row['player_name'],
                'league': row['league'],
                'season': row['season'],
                'confidence': 'exact',
                'match_score': None,
                'raw_team_name': row['raw_team_name'],
                'canonical_team': row['canonical_team'],
                # In-memory only — consumed by _dedup_canonical_per_season,
                # not written to Iceberg (_value_tuple skips it).
                'bronze_signal': row.get('bronze_signal', -1.0),
            }
        )
        stats['fbref']['total'] += 1
        stats['fbref']['resolved'] += 1

    # Cascade for non-FBref sources.
    fm_rows = fm_rows or []
    tm_rows = tm_rows or []
    cap_rows = cap_rows or []
    sf_rows = sf_rows or []
    es_rows = es_rows or []
    # Phase 1 — cascade. Ambiguous rows are deferred (DOB corroboration in
    # Phase 3b may still promote them); everything else is emitted as before.
    pending: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for src_rows in (us_rows, ws_rows, fm_rows, ss_rows, tm_rows, cap_rows,
                     sf_rows, es_rows):
        for row in src_rows:
            ambiguity_info: Dict[str, Any] = {}
            cid, conf, score = cascade_resolve(
                row, spine, nn=nn, ambiguity_out=ambiguity_info
            )
            stats[row['source']]['total'] += 1

            if conf == 'ambiguous':
                pending.append((row, ambiguity_info))
                continue

            out.append(
                {
                    'canonical_id': cid,
                    'source': row['source'],
                    'source_id': row['source_id'],
                    'display_name': row['player_name'],
                    'league': row['league'],
                    'season': row['season'],
                    'confidence': conf,
                    'match_score': score,
                    'raw_team_name': row['raw_team_name'],
                    'canonical_team': row['canonical_team'],
                    # In-memory only — consumed by _dedup_canonical_per_season,
                    # not written to Iceberg (_value_tuple skips it).
                    'bronze_signal': row.get('bronze_signal', -1.0),
                    # In-memory only — DOB corroboration (Phases 2-3).
                    'dob': row.get('dob'),
                }
            )
            if conf == 'orphan':
                stats[row['source']]['orphan'] += 1
            else:
                stats[row['source']]['resolved'] += 1

    # Phase 2 — consolidate per-canonical DOB from anchor-tier rows. Vetoable
    # tiers other than name_team never feed the map; a name_team row cannot
    # contradict its own contribution, and any two-source disagreement drops
    # the canonical from the map (recorded as a conflict) — see
    # DOB_VETO_TIERS docstring.
    canonical_dob, dob_conflicts = build_canonical_dob_map(out)
    dob_stats: Dict[str, Any] = {
        'canonical_dob_map': len(canonical_dob),
        'dob_conflicts': len(dob_conflicts),
        'promoted_from_review': 0,
        'vetoed': 0,
    }

    # Phase 3a — veto: fuzzy link whose DOB contradicts the canonical's.
    if canonical_dob:
        kept: List[Dict[str, Any]] = []
        for row in out:
            if (
                row['source'] != 'fbref'
                and row.get('confidence') in DOB_VETO_TIERS
                and row.get('dob') is not None
            ):
                known = canonical_dob.get(row['canonical_id'])
                if known is not None and not _dob_close(row['dob'], known):
                    pid = row['canonical_id'][3:]
                    spine_name = (spine.by_id.get(pid) or {}).get('player_name') or ''
                    review.append(_build_review_row(
                        {
                            'source': row['source'],
                            'source_id': row['source_id'],
                            'player_name': row['display_name'],
                            'raw_team_name': row.get('raw_team_name'),
                            'canonical_team': row.get('canonical_team'),
                            'league': row.get('league'),
                            'season': row.get('season'),
                        },
                        {
                            'rule': 'dob_veto',
                            'candidates': [(
                                pid,
                                spine_name,
                                float(row.get('match_score') or 0.0),
                            )],
                            'best_score': float(row.get('match_score') or 0.0),
                        },
                        detected_at,
                    ))
                    stats[row['source']]['resolved'] -= 1
                    stats[row['source']]['ambiguous'] += 1
                    dob_stats['vetoed'] += 1
                    continue
            kept.append(row)
        out = kept

    # Phase 3b — adjudicate deferred ambiguous rows.
    for row, ambiguity_info in pending:
        promoted = (
            adjudicate_ambiguous_with_dob(row, ambiguity_info, canonical_dob)
            if canonical_dob else None
        )
        if promoted is not None:
            cid, conf, score = promoted
            out.append(
                {
                    'canonical_id': cid,
                    'source': row['source'],
                    'source_id': row['source_id'],
                    'display_name': row['player_name'],
                    'league': row['league'],
                    'season': row['season'],
                    'confidence': conf,
                    'match_score': score,
                    'raw_team_name': row['raw_team_name'],
                    'canonical_team': row['canonical_team'],
                    'bronze_signal': row.get('bronze_signal', -1.0),
                    'dob': row.get('dob'),
                }
            )
            stats[row['source']]['resolved'] += 1
            dob_stats['promoted_from_review'] += 1
        else:
            review.append(_build_review_row(row, ambiguity_info, detected_at))
            stats[row['source']]['ambiguous'] += 1

    if dob_stats_out is not None:
        dob_stats_out.update(dob_stats)
        dob_stats_out['conflicts'] = dob_conflicts

    return out, review, stats


#: #803/#788: confidence-tier strength for canonical-collision resolution
#: (smaller = stronger). When two different players land on one canonical in a
#: season (ESPN namesakes #803, or Transfermarkt false matches on the thin
#: historical spine #788), the strongest-tier match owns it; the others are
#: demoted to orphan. Mirrors the resolver tier cascade order.
_TIER_RANK: Dict[str, int] = {
    'exact': 0,
    'name_team': 1,
    'name_team_alias': 2,
    # DOB-corroborated identity outranks the weak fuzzy tiers: when a
    # canonical collision pits a name_team_dob row against e.g. a
    # name_team_surname row, the birth-date-confirmed link owns the canonical.
    'name_team_dob': 3,
    'name_team_nickname': 4,
    'name_team_subset': 5,
    'name_team_surname': 6,
    'name_team_jersey': 7,
}


def _espn_identity(row: Dict[str, Any]) -> str:
    """Player identity behind an ESPN ``source_id`` (``'<name>|<team>'``).

    The name part is the stable identity; the team part varies across
    within-season transfers. Used by :func:`_dedup_canonical_per_season` to
    tell a legit multi-team stint (same name, #720) from a surname-tier false
    match (different names) sharing one canonical (#803).
    """
    return str(row.get('source_id', '')).split('|', 1)[0]


def _dedup_canonical_per_season(
    rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """Collapse multiple ``source_id`` rows per ``(canonical_id, source,
    league, season)`` down to one (issue #70).

    A single canonical_id can legitimately bind to several source_ids in the
    same season — Understat exposes Harrison Reed as both 910 and 6827, FotMob
    profiles get split across in-season transfers, etc. When a downstream Gold
    fact JOINs ``silver.xref_player`` on ``(source, source_id)`` without
    ``(league, season)``, the duplicate rows fan-out 2×; the prior workaround
    was a ROW_NUMBER CTE inside ``fct_player_match.sql``.

    Tiebreaker (largest wins):
      1. ``bronze_signal`` — minutes_played / games proxy from each source's
         Bronze. ``-1.0`` sentinel for sources with no proxy (TM, Capology)
         degenerates this rank.
      2. ``int(source_id)`` if the string is purely numeric, else 0.
      3. lexicographic ``source_id`` — last-resort deterministic order.

    Orphan rows (``confidence == 'orphan'``) are passed through unchanged —
    their canonical_id is source-private (``orphan:<...>``) so they cannot
    fan-out a real canonical in Gold.

    ESPN gets per-identity handling (#720 + #803). ESPN has no native
    player_id, so ``source_id`` is the ``'<name>|<team>'`` composite and a
    within-season transfer resolves both club-stints to the SAME canonical_id.
    Its only downstream consumer — ``gold/fct_lineup.sql`` — JOINs on
    ``(display_name, raw_team_name, league, season)``, which disambiguates by
    club, so two stints of the SAME name are legitimate, NOT the fan-out this
    function targets, and both survive (#720). But on a thin historical FBref
    spine the surname tier can bind a DIFFERENT player onto an existing
    canonical (e.g. ``Steven Sessegnon|Fulham`` onto Ryan's ``fb_…``). When a
    canonical carries ≥2 distinct name-identities in one season, the
    strongest-confidence-tier identity owns it (keeping all its club-stints)
    and the rest are demoted to orphan (#803) so they cannot pollute the real
    canonical. ESPN Bronze rows are unique per ``(player, team, league,
    season)`` (``_fetch_espn_players`` GROUP BY), so same-name pass-through
    introduces no true duplicates.

    Returns ``(deduped_rows, removed_per_source)``.
    """
    from collections import defaultdict

    out: List[Dict[str, Any]] = []
    groups: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    # #803: ESPN is no longer blanket-exempt — collected separately so a
    # canonical bound to two DIFFERENT players in one season can be resolved
    # (same-name multi-team stints still pass through, see below).
    espn_groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    # #788: ALL Transfermarkt rows (resolved + orphan) go to a two-pass dedup
    # below. TM Bronze is per-(player, club, season), so one player_id with a
    # mid-season transfer yields two anchor rows that can resolve DIFFERENTLY
    # (one to FBref, the other orphan / to a different fb_id). The PK is
    # (source, source_id, league, season) — no club — so they MUST collapse to
    # one row, else a PK duplicate. Collected here (TM-check before the orphan
    # passthrough so TM orphans land in the same dedup).
    tm_rows: List[Dict[str, Any]] = []
    for row in rows:
        if row.get('source') == 'transfermarkt':
            tm_rows.append(row)
            continue
        if row.get('confidence') == 'orphan':
            out.append(row)
            continue
        if row.get('source') == 'espn':
            espn_groups[(
                row['canonical_id'],
                row['league'],
                str(row['season']),
            )].append(row)
            continue
        key = (
            row['canonical_id'],
            row['source'],
            row['league'],
            str(row['season']),
        )
        groups[key].append(row)

    def _tie_key(r: Dict[str, Any]) -> Tuple[float, int, str]:
        sig = r.get('bronze_signal')
        sig_f = float(sig) if sig is not None else -1.0
        sid_raw = r.get('source_id') or ''
        sid_str = str(sid_raw)
        try:
            sid_int = int(sid_str)
        except (TypeError, ValueError):
            sid_int = 0
        return (sig_f, sid_int, sid_str)

    removed: Dict[str, int] = defaultdict(int)
    for members in groups.values():
        if len(members) == 1:
            out.append(members[0])
            continue
        winner = max(members, key=_tie_key)
        out.append(winner)
        removed[winner['source']] += len(members) - 1

    # #803: ESPN canonical-collision resolution. One name-identity per canonical
    # per season is the invariant; ≥2 distinct identities ⇒ keep the
    # strongest-tier owner (all its club-stints) and demote the rest to orphan
    # so a surname-tier false match cannot pollute a real canonical in Gold.
    for members in espn_groups.values():
        if len({_espn_identity(m) for m in members}) <= 1:
            out.extend(members)
            continue
        best_rank = min(
            _TIER_RANK.get(m.get('confidence'), 99) for m in members
        )
        contenders = [
            m for m in members
            if _TIER_RANK.get(m.get('confidence'), 99) == best_rank
        ]
        owner = _espn_identity(max(contenders, key=_tie_key))
        for m in members:
            if _espn_identity(m) == owner:
                out.append(m)
            else:
                demoted = dict(m)
                demoted['canonical_id'] = f"{_orphan_prefix('espn')}_{m['source_id']}"
                demoted['confidence'] = 'orphan'
                out.append(demoted)
                removed['espn'] += 1

    # #788: Transfermarkt two-pass dedup.
    # Pass 1 (PK): one row per (source_id, league, season). A mid-season transfer
    # gives a player_id two anchor rows (different clubs) that resolve
    # differently; the PK has no club, so collapse to ONE — prefer the
    # strongest-tier RESOLVED stint (a player found in any club is resolved),
    # else a single orphan. Without this, a resolved stint + an orphan stint of
    # the same player_id are a PK duplicate (#788 live: 141 dups, all TM).
    tm_pk: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in tm_rows:
        tm_pk[(str(r['source_id']), r['league'], str(r['season']))].append(r)

    def _tm_pk_key(r: Dict[str, Any]) -> Tuple[int, int, float, int]:
        # Prefer resolved over orphan, then stronger tier, then _tie_key (signal,
        # source_id) — negated so a single ``min`` picks the best stint.
        is_orphan = 0 if r.get('confidence') != 'orphan' else 1
        tier = _TIER_RANK.get(r.get('confidence'), 99)
        sig_f, sid_int, _ = _tie_key(r)
        return (is_orphan, tier, -sig_f, -sid_int)

    tm_pk_winners: List[Dict[str, Any]] = []
    for members in tm_pk.values():
        if len(members) > 1:
            removed['transfermarkt'] += len(members) - 1
        tm_pk_winners.append(min(members, key=_tm_pk_key))

    # Pass 2 (fan-out): ≥2 DISTINCT source_ids on one canonical/season is a false
    # fuzzy-match on the thin spine — keep the strongest-tier owner, demote the
    # rest to tm_<source_id> orphans so they don't fan-out the real canonical.
    tm_canon: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in tm_pk_winners:
        if r.get('confidence') == 'orphan':
            out.append(r)
            continue
        tm_canon[(r['canonical_id'], r['league'], str(r['season']))].append(r)

    for members in tm_canon.values():
        if len(members) == 1:
            out.append(members[0])
            continue
        best_rank = min(
            _TIER_RANK.get(m.get('confidence'), 99) for m in members
        )
        contenders = [
            m for m in members
            if _TIER_RANK.get(m.get('confidence'), 99) == best_rank
        ]
        owner_id = str(max(contenders, key=_tie_key)['source_id'])
        for m in members:
            if str(m['source_id']) == owner_id:
                out.append(m)
            else:
                demoted = dict(m)
                demoted['canonical_id'] = f"{_orphan_prefix('transfermarkt')}_{m['source_id']}"
                demoted['confidence'] = 'orphan'
                out.append(demoted)
                removed['transfermarkt'] += 1

    return out, dict(removed)


#: Core hard-gate source set — the original R2 regression contract.
_KNOWN_PAIR_CORE_SOURCES = frozenset({'fbref', 'understat', 'whoscored'})

#: Extended WARNING-only gate: SofaScore names come from a possibly-sparse
#: profile JOIN and FotMob has a separate ingest, so these were historically
#: excluded from the hard assertion. Verified softly until live pass-rates
#: justify promotion (see KNOWN_PAIR_EXT_MIN_PASS).
_KNOWN_PAIR_EXT_SOURCES = frozenset({'fbref', 'sofascore', 'fotmob'})


def _verify_known_pairs(
    rows: List[Dict[str, Any]],
    required_sources: frozenset = _KNOWN_PAIR_CORE_SOURCES,
    pairs: Optional[Tuple[Tuple[str, str], ...]] = None,
) -> Tuple[int, int]:
    """Return (passed, total). A pair "passes" iff the expected canonical_id
    appears with at least one row from each of ``required_sources``.

    Done in-memory rather than as a Trino query so the regression check
    works even when the INSERT step is mocked out (e.g. unit tests).

    The default (core) source set is the hard ResolverError gate; the
    extended set (:data:`_KNOWN_PAIR_EXT_SOURCES`) is evaluated WARNING-only
    by :func:`run_resolver`. ``pairs`` defaults to the APL anchor set
    (:data:`KNOWN_PAIRS`); :func:`run_resolver` passes the league-scoped set
    from :data:`KNOWN_PAIRS_BY_LEAGUE`.
    """
    if pairs is None:
        pairs = KNOWN_PAIRS
    by_cid: Dict[str, set] = {}
    for r in rows:
        by_cid.setdefault(r['canonical_id'], set()).add(r['source'])
    passed = 0
    for _, expected_cid in pairs:
        sources = by_cid.get(expected_cid, set())
        if required_sources <= sources:
            passed += 1
    return passed, len(pairs)


# ---------------------------------------------------------------------------
# Season helpers
# ---------------------------------------------------------------------------
def _slug_to_fbref_year(slug: int) -> int:
    """Convert season slug (e.g. 2425 for 2024-25) to FBref year-of-start (2024).

    YAML/Bronze varchar use slug; FBref Bronze stores integer year-of-start.
    Mapping: ``slug // 100 + 2000``.

        2122 -> 2021
        2425 -> 2024
        2526 -> 2025

    Raises:
        ValueError: if slug is not a 4-digit ``yyXX`` value.
    """
    if slug < 100:
        raise ValueError(f"season slug must be 4-digit yyXX (got {slug})")
    return (slug // 100) + 2000


def _fbref_year_to_slug(year) -> str:
    """Inverse of _slug_to_fbref_year. 2024 -> '2425', '2024' -> '2425'."""
    y = int(year)
    return f"{(y - 2000):02d}{(y - 2000 + 1):02d}"


def _split_seasons(slugs: List[int]) -> Tuple[List[int], List[str]]:
    """Map slug list ``[2425]`` -> ``(fbref=[2024], legacy=['2425'])``.

    Conventions across the platform:
      * YAML / public resolver API     -> 4-digit slug ``yyXX`` (``2425``).
      * FBref Bronze ``season`` (bigint) -> year-of-start (``2024``).
      * Understat / WhoScored Bronze ``season`` (varchar) -> slug (``'2425'``).

    The split is what lets FBref filters use ``season IN (2024)`` while
    Understat/WhoScored filters use ``season IN ('2425')`` from a single
    YAML-derived input list.
    """
    fbref: List[int] = []
    legacy: List[str] = []
    for slug in slugs:
        slug_int = int(slug)
        fbref.append(_slug_to_fbref_year(slug_int))
        legacy.append(f"{slug_int:04d}")
    return fbref, legacy


def _default_seasons_from_config(league: str) -> List[int]:
    """Pull every configured season for the given league out of competitions.yaml.

    Failure modes:
      * League not in catalog -> KeyError (loud — caller passed a typo).
      * League present but seasons list empty -> raises ResolverError
        (silent empty would produce a 0-row xref, masking a real config bug).
    """
    from utils.medallion_config import get_competition_seasons

    seasons = get_competition_seasons(league)
    if not seasons:
        raise ResolverError(
            f"competitions.yaml has no seasons for league={league!r} — "
            "refusing to materialise an empty xref_player."
        )
    return list(seasons)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def _build_nicknamer() -> Any:
    """Instantiate ``nicknames.NickNamer`` lazily.

    Returns ``None`` when the package is unavailable so the cascade simply
    skips tier 2.7 rather than crashing — useful for environments where
    the medallion image hasn't been rebuilt yet (dev / smoke).
    """
    try:
        from nicknames import NickNamer  # type: ignore
    except Exception as e:  # pragma: no cover — exercised at deploy boundary
        logger.warning(
            "nicknames package unavailable (%s); tier 2.7 (nickname dict) "
            "will be skipped this run.",
            e,
        )
        return None
    return NickNamer()


def run_resolver(
    target_table: str = DEFAULT_TARGET_TABLE,
    league: str = 'ENG-Premier League',
    seasons: Optional[List[int]] = None,
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    drop_before_insert: bool = True,
    review_table: str = DEFAULT_REVIEW_TABLE,
    materialize_review: bool = True,
    write_mode: str = 'rebuild',
) -> Dict[str, Any]:
    """Full pipeline: read 5 Bronze sources -> resolve -> write Iceberg.

    Args:
        target_table: Fully-qualified Iceberg table for the canonical
            xref_player rows.
        league: League id from ``competitions.yaml``. The APL default is a
            CLI/debug convenience only — the DAG always passes the league
            explicitly (it loops over ``get_in_scope_competitions()``).
        seasons: List of season slugs (e.g. ``[2425]`` for 2024-25). Use
            slug format (``yyXX``), NOT year-of-start. ``None`` means
            "all configured seasons for ``league``" pulled from
            ``competitions.yaml``.
        chunk_size: Rows per ``INSERT VALUES`` batch.
        drop_before_insert: If True (default), the Iceberg tables are
            (re)written according to ``write_mode``. Set False only if you
            want to run the resolver as a smoke-test without touching
            Iceberg at all (mostly useful in dual-run validation).
        write_mode: ``'rebuild'`` (default) — DROP TABLE + CREATE + INSERT,
            the historical single-league semantics. ``'replace_league'`` —
            ``DELETE FROM <table> WHERE league = <league>`` + INSERT, so a
            multi-league loop can materialise league N without erasing
            league N-1 (the DAG uses rebuild for the first league of a run
            and replace_league for the rest). Only consulted when
            ``drop_before_insert`` is True.
        review_table: Fully-qualified Iceberg table for the Fellegi-Sunter
            clerical-review band rows.
        materialize_review: If True (default), DROP+CREATE+INSERT the
            review table from the same run. Set False only when running
            against a fixture where review rows aren't expected.

    Returns:
        Summary dict::

            {
                'target_table':  'iceberg.silver.xref_player',
                'review_table':  'iceberg.silver.xref_player_review',
                'rows_inserted': 1615,
                'review_inserted': 8,
                'per_source': {
                    'fbref':     {'total': 562, 'resolved': 562,
                                  'orphan': 0,  'ambiguous': 0,
                                  'rejection_pct': 0.0,
                                  'review_pct': 0.0},
                    'understat': {'total': 562, 'resolved': 549,
                                  'orphan': 9,  'ambiguous': 4,
                                  'rejection_pct': 1.6,
                                  'review_pct':    0.7},
                    ...
                },
                'known_pair_pass_rate': '10/10',
                'duration_sec': 7.4,
            }

    Raises:
        ResolverError: when the known-pair regression check returns
            < KNOWN_PAIR_MIN_PASS (8/10).
    """
    from datetime import datetime, timezone

    if write_mode not in ('rebuild', 'replace_league'):
        raise ValueError(
            f"write_mode must be 'rebuild' or 'replace_league', got {write_mode!r}"
        )

    started = time.time()
    detected_at = datetime.now(tz=timezone.utc)

    if seasons is None:
        seasons = _default_seasons_from_config(league)
    fbref_seasons, source_seasons = _split_seasons(seasons)

    logger.info(
        "Starting xref_player v2 resolver: target=%s review=%s league=%s "
        "seasons=%s (fbref=%s, source=%s)",
        target_table,
        review_table,
        league,
        seasons,
        fbref_seasons,
        source_seasons,
    )

    nn = _build_nicknamer()

    conn = _get_trino_connection()
    try:
        logger.info("Reading FBref players ...")
        fb = _fetch_fbref_players(conn, league, fbref_seasons)
        logger.info("  %d FBref players", len(fb))

        logger.info("Reading Understat players ...")
        us = _fetch_understat_players(conn, league, source_seasons)
        logger.info("  %d Understat players", len(us))

        logger.info("Reading WhoScored players ...")
        ws = _fetch_whoscored_players(conn, league, source_seasons)
        logger.info("  %d WhoScored players", len(ws))

        logger.info("Reading FotMob players ...")
        fm = _fetch_fotmob_players(conn, league, fbref_seasons)
        logger.info("  %d FotMob players", len(fm))

        logger.info("Reading SofaScore players ...")
        ss = _fetch_sofascore_players(conn, league, source_seasons)
        logger.info("  %d SofaScore players", len(ss))

        # #788: resolve TM across ALL seasons (was current-season-only in #803).
        # The per-canonical fan-out that motivated the restriction (241 collisions
        # over the 10-season #793 backfill) is now handled in
        # _dedup_canonical_per_season: distinct TM player_ids false-matched onto
        # one canonical demote to tm_<id> orphans instead of duplicating it.
        logger.info("Reading Transfermarkt players ...")
        tm = _fetch_transfermarkt_players(conn, league, source_seasons)
        logger.info("  %d Transfermarkt players", len(tm))

        logger.info("Reading Capology players ...")
        cap = _fetch_capology_players(conn, league, source_seasons)
        logger.info("  %d Capology players", len(cap))

        logger.info("Reading SoFIFA players ...")
        sf = _fetch_sofifa_players(conn, league, source_seasons)
        logger.info("  %d SoFIFA players", len(sf))

        logger.info("Reading ESPN players ...")
        es = _fetch_espn_players(conn, league, source_seasons)
        logger.info("  %d ESPN players", len(es))

        # DOB corroboration input — annotate candidates with their Bronze
        # date-of-birth (in-memory field like bronze_signal; never written).
        logger.info("Reading Bronze DOB maps ...")
        dob_maps = _fetch_dob_maps(conn, league, source_seasons)
        candidates_with_dob = 0
        for src_rows, src in (
            (fm, 'fotmob'), (ss, 'sofascore'), (tm, 'transfermarkt'),
            (sf, 'sofifa'), (ws, 'whoscored'),
        ):
            m = dob_maps.get(src) or {}
            if not m:
                continue
            for r in src_rows:
                dob = m.get(str(r['source_id']))
                if dob is not None:
                    r['dob'] = dob
                    candidates_with_dob += 1
        logger.info("  %d candidates annotated with DOB", candidates_with_dob)

        logger.info("Resolving identities ...")
        dob_stats: Dict[str, Any] = {}
        rows, review_rows, stats = _resolve_all(
            fb, us, ws, ss, fm, tm, cap, sf, es, nn=nn,
            detected_at=detected_at, dob_stats_out=dob_stats,
        )
        logger.info(
            "  produced %d xref rows, %d review rows",
            len(rows),
            len(review_rows),
        )

        # Issue #70: collapse multi-source_id-per-canonical fan-out at the
        # source so downstream Gold facts don't need ROW_NUMBER hacks.
        rows, dedup_removed = _dedup_canonical_per_season(rows)
        if dedup_removed:
            logger.info(
                "  dedup_canonical_per_season removed %s",
                dedup_removed,
            )

        # Regression guard — done before INSERT so a failure aborts without
        # touching the Iceberg table.
        # Bind to dedicated names so the per-source `total` rebind below
        # cannot shadow these (was a real bug — summary showed e.g. "10/491").
        league_pairs = KNOWN_PAIRS_BY_LEAGUE.get(league)
        if not league_pairs:
            # Multi-league prep: a league without curated anchors SKIPS the
            # gate (WARNING) instead of failing — curate anchors when the
            # league's data settles, then this branch stops firing.
            logger.warning(
                "No known-pair anchors configured for league=%r — "
                "regression gate SKIPPED this run.", league,
            )
            known_passed = known_total = None
            ext_passed = ext_total = None
        else:
            known_passed, known_total = _verify_known_pairs(
                rows, pairs=league_pairs
            )
            if known_passed < KNOWN_PAIR_MIN_PASS:
                raise ResolverError(
                    f"Known-pair regression: {known_passed}/{known_total} passed, "
                    f"target ≥{KNOWN_PAIR_MIN_PASS}/{known_total}. "
                    "Inspect alias YAML / threshold tuning before retrying."
                )

            # Extended gate (FBref+SofaScore+FotMob) — WARNING-only for now,
            # see KNOWN_PAIR_EXT_MIN_PASS. Never aborts the run.
            ext_passed, ext_total = _verify_known_pairs(
                rows, required_sources=_KNOWN_PAIR_EXT_SOURCES,
                pairs=league_pairs,
            )
            if ext_passed < KNOWN_PAIR_EXT_MIN_PASS:
                logger.warning(
                    "Extended known-pair gate (fbref+sofascore+fotmob): %d/%d "
                    "passed, soft target ≥%d/%d — NOT failing the run "
                    "(WARNING-only gate).",
                    ext_passed, ext_total, KNOWN_PAIR_EXT_MIN_PASS, ext_total,
                )

        rows_inserted = 0
        review_inserted = 0
        if drop_before_insert:
            if write_mode == 'replace_league':
                # Multi-league loop: erase only THIS league's partition so
                # previously-materialised leagues survive the INSERT below.
                logger.info(
                    "Replacing league partition %r in %s ...",
                    league, target_table,
                )
                _execute(
                    conn,
                    f"DELETE FROM {target_table} "
                    f"WHERE league = '{_sql_escape(league)}'",
                )
            else:
                logger.info("Rewriting Iceberg target %s ...", target_table)
                _create_target_table(conn, target_table)
            rows_inserted = _insert_rows(conn, target_table, rows, chunk_size)
            logger.info("  inserted %d rows into %s", rows_inserted, target_table)

            if materialize_review:
                if write_mode == 'replace_league':
                    logger.info(
                        "Replacing league partition %r in %s ...",
                        league, review_table,
                    )
                    _execute(
                        conn,
                        f"DELETE FROM {review_table} "
                        f"WHERE league = '{_sql_escape(league)}'",
                    )
                else:
                    logger.info("Rewriting Iceberg review %s ...", review_table)
                    _create_review_table(conn, review_table)
                if review_rows:
                    review_inserted = _insert_review_rows(
                        conn, review_table, review_rows, chunk_size
                    )
                logger.info(
                    "  inserted %d review rows into %s",
                    review_inserted,
                    review_table,
                )
        else:
            logger.info(
                "drop_before_insert=False — skipping Iceberg write "
                "(dry-run / smoke mode)."
            )

    finally:
        try:
            conn.close()
        except Exception:
            pass

    # Build summary
    per_source: Dict[str, Dict[str, Any]] = {}
    for src, st in stats.items():
        total = st['total']
        ambiguous = st.get('ambiguous', 0)
        orphan = st['orphan']
        rejection_pct = round(100.0 * orphan / total, 2) if total else 0.0
        review_pct = round(100.0 * ambiguous / total, 2) if total else 0.0
        per_source[src] = {
            'total': total,
            'resolved': st['resolved'],
            'orphan': orphan,
            'ambiguous': ambiguous,
            'rejection_pct': rejection_pct,
            'review_pct': review_pct,
        }

    summary = {
        'target_table': target_table,
        'review_table': review_table,
        'rows_inserted': rows_inserted,
        'review_inserted': review_inserted,
        'per_source': per_source,
        'dedup_removed_per_source': dedup_removed,
        'known_pair_pass_rate': (
            'skipped' if known_passed is None
            else f"{known_passed}/{known_total}"
        ),
        'known_pair_pass_rate_ext': (
            'skipped' if ext_passed is None
            else f"{ext_passed}/{ext_total}"
        ),
        'dob': {
            'candidates_with_dob': candidates_with_dob,
            'canonical_dob_map': dob_stats.get('canonical_dob_map', 0),
            'dob_conflicts': dob_stats.get('dob_conflicts', 0),
            'promoted_from_review': dob_stats.get('promoted_from_review', 0),
            'vetoed': dob_stats.get('vetoed', 0),
        },
        'duration_sec': round(time.time() - started, 2),
    }
    logger.info("Resolver summary: %s", summary)
    return summary
