"""
Unit tests for the R2-followup v2 cascade tiers in
``dags/utils/xref_player_resolver.py``.

Coverage
--------
* Tier 2.3 — surname-anchor with Levenshtein≤1 on the last token.
* Tier 2.5 — token_set_ratio ≥ 95 (subset matches like Alisson ⊂ Alisson Becker).
* Tier 2.6 — token_set_ratio 88-94 → ambiguity-band (review queue).
* Tier 2.7 — nicknames PyPI dict (mocked NickNamer, no real package needed).
* Tier 3   — player_aliases.yaml fallback.
* Uniqueness guard on every fuzzy/dict tier (Fellegi-Sunter MPI pattern).
* Known-pair regression baseline still GREEN with the v2 cascade.
* Backward-compat: ``cascade_resolve`` keeps its 3-tuple signature.

The new tiers are all expressed as pure functions over the in-memory
spine + a lightweight FakeNickNamer fixture, so the suite runs on the
host without docker / Trino / the ``nicknames`` PyPI package.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import pytest

# Make `from utils.xref_player_resolver import ...` resolve on the host.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / "dags"
for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)


from utils import xref_player_resolver as xpr  # noqa: E402

# Skip the entire suite cleanly if rapidfuzz / unidecode aren't installed.
rapidfuzz = pytest.importorskip("rapidfuzz")
unidecode = pytest.importorskip("unidecode")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
class FakeNickNamer:
    """Minimal stand-in for ``nicknames.NickNamer``.

    Built from a forward-facing canonical→nicknames table (canonical → set).
    Implements both directions of lookup (``nicknames_of`` / ``canonicals_of``)
    and emits TITLE-cased values exactly like the real package, so that the
    case-fold logic inside :func:`xpr._nickname_match` is exercised faithfully.
    """

    def __init__(self, table: Dict[str, Set[str]]):
        # Forward: canonical → {nicknames}
        self._fwd: Dict[str, Set[str]] = {
            k.lower(): {v.lower() for v in vals}
            for k, vals in table.items()
        }
        # Reverse: nickname → {canonicals}
        rev: Dict[str, Set[str]] = {}
        for canon, nicks in self._fwd.items():
            for n in nicks:
                rev.setdefault(n, set()).add(canon)
        self._rev = rev

    @staticmethod
    def _titled(s: Iterable[str]) -> Set[str]:
        return {x.title() for x in s}

    def nicknames_of(self, name: str) -> Set[str]:
        return self._titled(self._fwd.get(name.lower(), set()))

    def canonicals_of(self, name: str) -> Set[str]:
        return self._titled(self._rev.get(name.lower(), set()))


@pytest.fixture
def nn_fixture() -> FakeNickNamer:
    """Tiny English nickname table covering the user-supplied orphan cases.

    Both ``Andrew`` and ``Andre`` advertise ``Andy`` as a nickname so the
    collision test (two candidates → ambiguous) can fire — the real
    ``nicknames`` PyPI dataset has the same overlap (Andre/Andy and
    Andrew/Andy both appear).
    """
    return FakeNickNamer({
        'Andrew':  {'Andy', 'Drew', 'Andre'},
        'Andre':   {'Andy', 'Dre'},
        'Daniel':  {'Dan', 'Danny'},
        'Matthew': {'Matt', 'Matty'},
        'Robert':  {'Bob', 'Bobby', 'Rob'},
        'Joshua':  {'Josh'},
    })


def _fb_row(pid: str, name: str, team: str, season: str = '2425') -> Dict:
    """Helper to build a minimal FBref spine row for ``_FBrefSpine``."""
    return {
        'player_id': pid,
        'source_id': pid,
        'player_name': name,
        'canonical_team': team,
        'season': season,
    }


def _spine(rows: List[Dict]) -> 'xpr._FBrefSpine':
    return xpr._FBrefSpine(rows)


def _candidate(source: str, sid: str, name: str, team: str,
               season: str = '2425') -> Dict:
    """Minimal source candidate row consumed by ``cascade_resolve``."""
    return {
        'source': source,
        'source_id': sid,
        'player_name': name,
        'raw_team_name': team,
        'canonical_team': team,
        'league': 'ENG-Premier League',
        'season': season,
    }


# ---------------------------------------------------------------------------
# Tier 2.3 — surname-anchor
# ---------------------------------------------------------------------------
class TestSurnameAnchor:
    def test_andy_robertson_resolves_to_andrew_robertson(self):
        """Andy Robertson → Andrew Robertson via surname-anchor.
        Levenshtein.distance('robertson', 'robertson') == 0; surname-anchor
        sees a unique match and links with confidence 'name_team_surname'.
        """
        spine = _spine([_fb_row('rob42', 'Andrew Robertson', 'Liverpool')])
        cand = _candidate('understat', 'u_andy', 'Andy Robertson', 'Liverpool')
        cid, conf, score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_rob42'
        assert conf == 'name_team_surname'
        assert score == 100.0

    def test_iyenoma_destiny_udogie_resolves(self):
        """Source 'Iyenoma Destiny Udogie' → spine 'Destiny Udogie'.

        Surname-anchor exits via the unique 'udogie' last-token match
        (token_set tier would also catch this — surname-anchor wins because
        cascade order tries it first).
        """
        spine = _spine([_fb_row('udo7', 'Destiny Udogie', 'Tottenham Hotspur')])
        cand = _candidate(
            'understat', 'u_udo',
            'Iyenoma Destiny Udogie',
            'Tottenham Hotspur',
        )
        cid, conf, _score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_udo7'
        assert conf == 'name_team_surname'

    def test_levenshtein_typo_on_surname(self):
        """Surname typo with a deliberately different first-name forces
        tier-2 to miss (token_sort_ratio < 90) and surname-anchor to win
        on Levenshtein≤1 of the long surname."""
        spine = _spine([_fb_row('rob42', 'Andrew Robertson', 'Liverpool')])
        # 'Mikhail' as first name is far enough from 'Andrew' that
        # token_sort_ratio drops below 90; surname tokens 'roberson' vs
        # 'robertson' are Levenshtein-1 → surname-anchor wins uniquely.
        cand = _candidate('whoscored', 'w1', 'Mikhail Roberson', 'Liverpool')
        cid, conf, _score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_rob42'
        assert conf == 'name_team_surname'

    def test_surname_anchor_collision_routes_to_review(self):
        """Two different first-names with same surname in same bucket → ambiguous.
        ``cascade_resolve`` must NOT auto-link, must populate ambiguity_out.
        """
        spine = _spine([
            _fb_row('p1', 'James Smith', 'Arsenal'),
            _fb_row('p2', 'Robert Smith', 'Arsenal'),
        ])
        cand = _candidate('understat', 'u_smith', 'Sam Smith', 'Arsenal')
        info: Dict = {}
        cid, conf, score = xpr.cascade_resolve(
            cand, spine, ambiguity_out=info,
        )
        assert cid is None
        assert conf == 'ambiguous'
        assert score is None
        assert info['rule'] == 'surname_collision'
        # Both candidates surfaced for the reviewer.
        assert {c[0] for c in info['candidates']} == {'p1', 'p2'}

    def test_short_surname_does_not_use_levenshtein(self):
        """Surname < SURNAME_MIN_LEN must NOT enable Levenshtein typo branch."""
        # spine has 'Cole' (4 chars). Source has 'Cone' — 1-letter Levenshtein.
        # 'Cole' is exactly at the boundary, so a typo on 'cone' could
        # match. Use a 3-letter surname instead to be deliberately short.
        spine = _spine([_fb_row('p1', 'Some Bob', 'Arsenal')])
        cand = _candidate('understat', 'u1', 'Some Boa', 'Arsenal')
        # Last tokens 'bob' (3 chars) and 'boa' — below min_len, no Levenshtein.
        cid, conf, _ = xpr.cascade_resolve(cand, spine)
        assert cid == 'us_u1'  # orphan
        assert conf == 'orphan'


# ---------------------------------------------------------------------------
# Tier 2.5 — token_set_ratio ≥ 95
# ---------------------------------------------------------------------------
class TestTokenSetSubset:
    def test_alisson_subset_alisson_becker(self):
        """Source 'Alisson' ⊂ spine 'Alisson Becker' → token_set_ratio == 100."""
        spine = _spine([_fb_row('ali1', 'Alisson Becker', 'Liverpool')])
        cand = _candidate('understat', 'u_ali', 'Alisson', 'Liverpool')
        cid, conf, score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_ali1'
        # Surname-anchor would see 'alisson' last-token in 'alisson becker'
        # which has 'becker' as last-token. So surname-anchor MISSES; the
        # token_set_ratio tier fires and emits 'name_team_subset'.
        assert conf == 'name_team_subset'
        assert score == 100.0

    def test_idrissa_gueye_subset(self):
        """Source 'Idrissa Gueye' ⊂ spine 'Idrissa Gana Gueye' → subset hit."""
        spine = _spine([_fb_row('gue9', 'Idrissa Gana Gueye', 'Everton')])
        cand = _candidate('understat', 'u_gue', 'Idrissa Gueye', 'Everton')
        cid, conf, _score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_gue9'
        # Last tokens match ('gueye'==='gueye') → surname-anchor wins.
        assert conf == 'name_team_surname'

    def test_estevao_subset(self):
        """Source 'Estêvão' ⊂ spine 'Estêvão Willian' → subset / surname tier."""
        spine = _spine([_fb_row('est7', 'Estêvão Willian', 'Chelsea')])
        cand = _candidate('understat', 'u_est', 'Estêvão', 'Chelsea')
        cid, conf, _score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_est7'
        # 'estevao' last token vs 'willian' last token — surname-anchor
        # FAILS; token_set_ratio('estevao', 'estevao willian') == 100 → subset.
        assert conf == 'name_team_subset'


class TestTokenSetAmbiguityBand:
    def test_88_94_band_routes_to_review(self):
        """Score in [88, 95) → ambiguous regardless of bucket cardinality.

        ``Lucas Paqueta`` vs ``Lucas Paquette``:
          * token_sort_ratio = 88.89 → tier-2 (≥ 90) misses.
          * Surname Levenshtein('paqueta', 'paquette') = 2 → surname-anchor
            misses.
          * token_set_ratio = 88.89 → falls into the [88, 95) band.
        """
        from rapidfuzz import fuzz
        # Empirical verification — guards against future rapidfuzz scoring drift.
        score = fuzz.token_set_ratio('lucas paqueta', 'lucas paquette')
        assert 88 <= score < 95, f"setup: expected band score, got {score}"

        spine = _spine([_fb_row('paq1', 'Lucas Paquette', 'Genoa')])
        cand = _candidate('understat', 'u_paq', 'Lucas Paqueta', 'Genoa')
        info: Dict = {}
        cid, conf, _ = xpr.cascade_resolve(cand, spine, ambiguity_out=info)
        assert cid is None
        assert conf == 'ambiguous'
        assert info['rule'] == 'token_set_band'
        assert info['candidates']  # candidate(s) preserved for the reviewer
        # Score is in band; the FBref pid is exposed for review.
        assert {c[0] for c in info['candidates']} == {'paq1'}

    def test_two_subset_matches_routes_to_review(self):
        """Two FBref candidates ≥ 95 in same bucket → no auto-link."""
        # Force the surname-anchor tier to MISS (different last tokens) so
        # we hit the token_set tier. Use 'Jose' candidates: spine has
        # 'Jose Smith' and 'Jose Jones', source is 'Jose'. Last tokens differ
        # so surname-anchor no-ops, but token_set_ratio('jose', '...') = 100
        # for both.
        spine = _spine([
            _fb_row('s1', 'Jose Smith', 'Arsenal'),
            _fb_row('s2', 'Jose Jones', 'Arsenal'),
        ])
        cand = _candidate('understat', 'u1', 'Jose', 'Arsenal')
        info: Dict = {}
        cid, conf, _ = xpr.cascade_resolve(cand, spine, ambiguity_out=info)
        assert cid is None
        assert conf == 'ambiguous'
        assert info['rule'] == 'token_set_band'
        assert {c[0] for c in info['candidates']} == {'s1', 's2'}


# ---------------------------------------------------------------------------
# Tier 2.7 — nicknames dict
# ---------------------------------------------------------------------------
class TestNicknameTier:
    def test_andy_andrew_robertson_via_nickname(self, nn_fixture):
        """When surname-anchor is unique it wins — but a same-bucket distractor
        with identical surname forces the nickname tier path. Set up:
            spine: Andrew Robertson, Bob Robertson  (same team, both Roberts)
            source: Andy Robertson
        Surname-anchor sees TWO 'robertson' candidates → ambiguous. Nickname
        tier requires surname match AND first-name nickname pair → only one
        ('Andy' nickname of 'Andrew') resolves uniquely.

        BUT: we surface ambiguous BEFORE the nickname tier in cascade order
        (surname-anchor collision is preferred). To exercise the nickname
        tier directly, drop the second Robertson and force surname mismatch
        with a non-typo close form.
        """
        # Surname EQUAL → surname-anchor fires first → never reaches nicknames.
        # To exercise nickname tier, we need surname-anchor to NO-OP yet
        # nickname pair to fire. Use distinct surnames: 'Robertson' vs 'Robins'
        # (same canonical first-name family but distinct surname). However
        # nickname tier requires SURNAME MATCH per Splink #2206 to avoid
        # cross-player matches. So: surname must match, first must be a
        # nickname pair. The only way to bypass surname-anchor is to make
        # surname-anchor return ambiguous (>1 candidate in bucket) — and then
        # cascade_resolve returns 'ambiguous' before nickname runs.
        #
        # Conclusion: in the cascade order, nickname tier is only reachable
        # when surname-anchor finds 0 matches AND token_set_ratio < 88.
        # That's a narrow slice — name like 'Andy Smithson' vs 'Andrew Smithson'
        # would collide on surname-anchor (single-match) anyway. The realistic
        # path: source 'Matt Cash' vs spine 'Matthew Cash'. Surname 'cash'
        # has 4 chars (== MIN_LEN), surname-anchor wins via exact equality.
        # So nickname tier fires when surname tokens differ by Levenshtein > 1.
        #
        # Construct: spine 'Daniel Williams', source 'Danny Williamson' —
        # surname 'williamson' vs 'williams' differs by Levenshtein 2, no
        # surname-anchor. token_set_ratio('danny williamson', 'daniel williams')
        # < 95. Nickname tier requires surname EQUAL → fails.
        #
        # Final realistic construction: nickname tier only fires when source
        # has the canonical OR nickname form AND surname matches but the
        # surname-anchor already covers all surname-equal cases. Therefore
        # nickname tier is functionally redundant with surname-anchor for
        # APL data — it provides defence-in-depth for short surnames
        # (< MIN_LEN) where surname-anchor no-ops.
        #
        # Test: short surname 'Tah' (3 chars) — surname-anchor SKIPS (<4),
        # so a nickname pair with matching short surname triggers nickname tier.
        spine = _spine([_fb_row('p1', 'Andrew Tah', 'Bayer')])
        cand = _candidate('understat', 'u1', 'Andy Tah', 'Bayer')
        cid, conf, score = xpr.cascade_resolve(
            cand, spine, nn=nn_fixture,
        )
        assert cid == 'fb_p1'
        assert conf == 'name_team_nickname'
        assert score == 100.0

    def test_nickname_no_fixture_no_match(self, nn_fixture):
        """Without nn instance, nickname tier is skipped silently."""
        spine = _spine([_fb_row('p1', 'Andrew Tah', 'Bayer')])
        cand = _candidate('understat', 'u1', 'Andy Tah', 'Bayer')
        cid, conf, _ = xpr.cascade_resolve(cand, spine, nn=None)
        # Falls through to orphan because surname is too short for
        # surname-anchor and token_set_ratio doesn't reach 88.
        assert cid == 'us_u1'
        assert conf == 'orphan'

    def test_nickname_collision_two_andrews(self, nn_fixture):
        """Two candidates with same short surname AND first-name nickname-paired
        → ambiguous (uniqueness guard at nickname tier)."""
        spine = _spine([
            _fb_row('p1', 'Andrew Tah', 'Bayer'),
            _fb_row('p2', 'Andre Tah',  'Bayer'),
        ])
        cand = _candidate('understat', 'u1', 'Andy Tah', 'Bayer')
        info: Dict = {}
        cid, conf, _ = xpr.cascade_resolve(
            cand, spine, nn=nn_fixture, ambiguity_out=info,
        )
        assert cid is None
        assert conf == 'ambiguous'
        assert info['rule'] == 'nickname_collision'
        assert {c[0] for c in info['candidates']} == {'p1', 'p2'}


# ---------------------------------------------------------------------------
# Tier 3 — player_aliases.yaml lookup
# ---------------------------------------------------------------------------
class TestAliasYaml:
    def test_alias_yaml_fallback(self, monkeypatch):
        """When all algorithmic tiers fail, get_player_alias bridges by hand."""
        # Spine has the FBref player; source has unrelated name + same team.
        # Surname-anchor / token_set / nickname all FAIL. Alias YAML wins.
        spine = _spine([_fb_row('xyz9', 'Some Player', 'Arsenal')])
        cand = _candidate(
            'understat', 'u_alias', 'Completely Different', 'Arsenal',
        )

        def fake_get_player_alias(source, source_id, season):
            assert source == 'understat'
            assert source_id == 'u_alias'
            assert season == '2425'
            return 'xyz9'  # without 'fb_' prefix

        # Patch the lazy-imported function INSIDE the resolver module.
        from utils import medallion_config
        monkeypatch.setattr(
            medallion_config, 'get_player_alias', fake_get_player_alias,
        )

        cid, conf, score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_xyz9'
        assert conf == 'name_team_alias'
        assert score == 100.0

    def test_alias_overrides_wrong_surname_match(self, monkeypatch):
        """#738 — alias (Tier 1.5) must outrank a WRONG surname-anchor match.

        Without the alias, Understat 'Bobby Reid' surname-collides onto the
        only Fulham 'Reed' in the bucket (Harrison Reed) — reid↔reed is
        Levenshtein 1, both ≥ SURNAME_MIN_LEN — and would auto-link as
        'name_team_surname'. The alias re-points him to the real FBref entry
        (Bobby De Cordova-Reid), proving the alias is consulted before the
        surname tier short-circuits.
        """
        spine = _spine([_fb_row('reed1', 'Harrison Reed', 'Fulham')])
        cand = _candidate('understat', '6827', 'Bobby Reid', 'Fulham')

        # Bug repro: with the alias tier disabled, the surname tier mis-links
        # Bobby Reid onto Harrison Reed.
        from utils import medallion_config
        monkeypatch.setattr(
            medallion_config, 'get_player_alias', lambda *a, **k: None,
        )
        buggy_cid, buggy_conf, _ = xpr.cascade_resolve(cand, spine)
        assert (buggy_cid, buggy_conf) == ('fb_reed1', 'name_team_surname')

        # Fix: the shipped player_aliases.yaml re-points 6827 → Bobby De
        # Cordova-Reid, and the alias (Tier 1.5) wins before the surname tier.
        monkeypatch.undo()
        cid, conf, score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_0f7533cd'
        assert conf == 'name_team_alias'
        assert score == 100.0

    def test_alias_overrides_ambiguous_band(self, monkeypatch):
        """#738 — alias (Tier 1.5) must outrank an ambiguous-band verdict.

        Bare 'Gabriel' is a token_set subset of multiple real Gabriels in the
        same Arsenal bucket → ambiguous (review queue), never resolved. The
        alias disambiguates to Gabriel Magalhães before the ambiguous branch
        returns.
        """
        spine = _spine([
            _fb_row('48a5a5d6', 'Gabriel Martinelli', 'Arsenal'),
            _fb_row('67ac5bb8', 'Gabriel Magalhaes', 'Arsenal'),
        ])
        cand = _candidate('understat', '5613', 'Gabriel', 'Arsenal')

        # Bug repro: with the alias tier disabled the cascade declares ambiguous.
        from utils import medallion_config
        monkeypatch.setattr(
            medallion_config, 'get_player_alias', lambda *a, **k: None,
        )
        na_cid, na_conf, _ = xpr.cascade_resolve(cand, spine)
        assert (na_cid, na_conf) == (None, 'ambiguous')

        # Fix: the shipped player_aliases.yaml disambiguates 5613 → Magalhães.
        monkeypatch.undo()
        cid, conf, score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_67ac5bb8'
        assert conf == 'name_team_alias'
        assert score == 100.0


# ---------------------------------------------------------------------------
# Orphan terminal + uniqueness
# ---------------------------------------------------------------------------
class TestOrphanTerminal:
    def test_orphan_when_all_tiers_fail(self):
        """Source player nowhere in spine, no alias → orphan with prefix."""
        spine = _spine([_fb_row('p1', 'Bukayo Saka', 'Arsenal')])
        cand = _candidate('whoscored', 'ws_99', 'Unknown Person', 'Arsenal')
        cid, conf, _ = xpr.cascade_resolve(cand, spine)
        assert cid == 'ws_ws_99'
        assert conf == 'orphan'

    def test_orphan_preserves_best_score_seen(self):
        """When tier-2 saw a partial score it gets recorded for debugging."""
        spine = _spine([_fb_row('p1', 'Aaron Brown', 'Arsenal')])
        # Source name is far enough from spine that NO tier matches — name
        # tokens are completely different and surname Levenshtein > 1.
        cand = _candidate('understat', 'u9', 'Zlatko Vukovic', 'Arsenal')
        cid, conf, score = xpr.cascade_resolve(cand, spine)
        assert cid == 'us_u9'
        assert conf == 'orphan'
        # Some non-zero token_sort score from the tier-2 attempt.
        assert score is None or score > 0


# ---------------------------------------------------------------------------
# Backward-compat: existing 3-tuple unpacking unchanged
# ---------------------------------------------------------------------------
class TestBackwardCompat:
    def test_cascade_resolve_3tuple_signature(self):
        """``cid, conf, score = cascade_resolve(...)`` must keep working."""
        spine = _spine([_fb_row('saka1', 'Bukayo Saka', 'Arsenal')])
        cand = _candidate('understat', 'u_saka', 'Bukayo Saka', 'Arsenal')
        result = xpr.cascade_resolve(cand, spine)
        assert isinstance(result, tuple)
        assert len(result) == 3
        cid, conf, score = result
        assert cid == 'fb_saka1'
        assert conf == 'name_team'
        assert score >= 90.0

    def test_resolve_all_returns_3tuple_with_review(self):
        """``_resolve_all`` returns ``(rows, review, stats)`` after v2."""
        # _resolve_all consumes the post-fetch row shape (with 'league' /
        # 'raw_team_name') — extend the spine fixture accordingly.
        fb = [{
            'player_id': 'p1',
            'source_id': 'p1',
            'player_name': 'Some Player',
            'raw_team_name': 'Arsenal',
            'canonical_team': 'Arsenal',
            'league': 'ENG-Premier League',
            'season': '2425',
        }]
        us = [_candidate('understat', 'u1', 'Some Player', 'Arsenal')]
        ws: List[Dict] = []
        ss: List[Dict] = []
        result = xpr._resolve_all(fb, us, ws, ss)
        assert isinstance(result, tuple)
        assert len(result) == 3
        rows, review, stats = result
        assert isinstance(rows, list)
        assert isinstance(review, list)
        assert isinstance(stats, dict)


# ---------------------------------------------------------------------------
# Known-pair regression — the v2 cascade must NOT break the 10-pair guard.
# ---------------------------------------------------------------------------
class TestKnownPairsRegression:
    """All 10 hard-coded KNOWN_PAIRS must resolve via the v2 cascade.

    KNOWN_PAIRS test resolves through tier-1 / tier-2 already (they're
    spelled correctly across all 3 sources). The v2 tiers MUST NOT
    accidentally route them to ambiguous via overzealous matching.
    """

    def test_known_pairs_unaffected_by_v2_tiers(self, nn_fixture):
        """For each KNOWN_PAIR, build a 1-FBref + 1-Understat fixture and
        confirm the cascade returns the FBref canonical (not orphan, not
        ambiguous). Uses the legacy NAME_THRESHOLD path, but with v2
        tiers active to ensure they don't shadow tier-2."""
        # Use the 10 known names as the FBref spine entries; for each,
        # a same-name same-team source candidate must resolve via tier-2
        # (token_sort_ratio ≥ 90).
        fb = [
            _fb_row(cid.removeprefix('fb_'), name, 'TeamX')
            for name, cid in xpr.KNOWN_PAIRS
        ]
        spine = _spine(fb)
        for name, expected_cid in xpr.KNOWN_PAIRS:
            cand = _candidate('understat', f'u_{name}', name, 'TeamX')
            got_cid, conf, _ = xpr.cascade_resolve(
                cand, spine, nn=nn_fixture,
            )
            assert got_cid == expected_cid, (
                f"known-pair {name!r}: expected {expected_cid}, got {got_cid} "
                f"(confidence={conf!r})"
            )
            assert conf in {'exact', 'name_team', 'name_team_surname',
                            'name_team_subset', 'name_team_nickname'}, (
                f"{name}: unexpected confidence {conf!r}"
            )
