"""
Unit tests for the DOB-corroboration tier (``name_team_dob`` / ``dob_veto``)
in ``dags/utils/xref_player_resolver.py``.

Coverage
--------
* ``_dob_close``            — tolerance semantics (missing / 0 / 1 / 2 days).
* ``build_canonical_dob_map`` — consensus, ±1-day cluster, >1-day conflict,
  anchor-tier / prefix / source filters.
* ``adjudicate_ambiguous_with_dob`` — unique-match promotion, multi-match /
  no-match / missing-DOB fallthrough.
* ``_resolve_all`` end-to-end:
  - namesake bucket split (#738 «3 Габриэля») promoted via name_team_dob;
  - 88-94 token_set band auto-adjudicated on DOB match;
  - dob_veto: fuzzy link with contradicting DOB demoted to review;
  - strict no-regression: fixtures without any ``dob`` keys produce output
    identical to the pre-DOB behaviour (no promotions, no vetoes).
* ``_TIER_RANK`` — name_team_dob outranks the weak fuzzy tiers.
* ``_verify_known_pairs`` — new ``required_sources`` parameter.

All tests are pure in-memory (no Trino / DuckDB).
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Dict, List

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / "dags"
for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from utils import xref_player_resolver as xpr  # noqa: E402

rapidfuzz = pytest.importorskip("rapidfuzz")
unidecode = pytest.importorskip("unidecode")


# ---------------------------------------------------------------------------
# Fixtures (mirror test_xref_player_resolver_v2.py)
# ---------------------------------------------------------------------------
def _fb_row(pid: str, name: str, team: str, season: str = '2425') -> Dict:
    return {
        'player_id': pid,
        'source_id': pid,
        'player_name': name,
        'raw_team_name': team,
        'canonical_team': team,
        'league': 'ENG-Premier League',
        'season': season,
    }


def _candidate(source: str, sid: str, name: str, team: str,
               season: str = '2425', dob: date = None) -> Dict:
    row = {
        'source': source,
        'source_id': sid,
        'player_name': name,
        'raw_team_name': team,
        'canonical_team': team,
        'league': 'ENG-Premier League',
        'season': season,
    }
    if dob is not None:
        row['dob'] = dob
    return row


def _resolve(fb: List[Dict], **src_rows) -> tuple:
    """Call ``_resolve_all`` with keyword source lists and dob stats out."""
    dob_stats: Dict = {}
    rows, review, stats = xpr._resolve_all(
        fb,
        src_rows.get('us', []),
        src_rows.get('ws', []),
        src_rows.get('ss', []),
        fm_rows=src_rows.get('fm', []),
        tm_rows=src_rows.get('tm', []),
        cap_rows=src_rows.get('cap', []),
        sf_rows=src_rows.get('sf', []),
        es_rows=src_rows.get('es', []),
        dob_stats_out=dob_stats,
    )
    return rows, review, stats, dob_stats


# ---------------------------------------------------------------------------
# _dob_close
# ---------------------------------------------------------------------------
class TestDobClose:
    def test_missing_either_side_is_no_signal(self):
        assert xpr._dob_close(None, date(2001, 6, 6)) is False
        assert xpr._dob_close(date(2001, 6, 6), None) is False
        assert xpr._dob_close(None, None) is False

    def test_exact_and_one_day_within_tolerance(self):
        assert xpr._dob_close(date(2001, 6, 6), date(2001, 6, 6)) is True
        assert xpr._dob_close(date(2001, 6, 6), date(2001, 6, 7)) is True
        assert xpr._dob_close(date(2001, 6, 7), date(2001, 6, 6)) is True

    def test_two_days_is_a_contradiction(self):
        assert xpr._dob_close(date(2001, 6, 6), date(2001, 6, 8)) is False


# ---------------------------------------------------------------------------
# build_canonical_dob_map
# ---------------------------------------------------------------------------
def _xref_row(cid: str, source: str, confidence: str, dob=None) -> Dict:
    return {
        'canonical_id': cid,
        'source': source,
        'confidence': confidence,
        'dob': dob,
    }


class TestBuildCanonicalDobMap:
    def test_consensus_two_sources(self):
        rows = [
            _xref_row('fb_a', 'fotmob', 'name_team', date(1998, 3, 1)),
            _xref_row('fb_a', 'sofascore', 'name_team', date(1998, 3, 1)),
        ]
        cmap, conflicts = xpr.build_canonical_dob_map(rows)
        assert cmap == {'fb_a': date(1998, 3, 1)}
        assert conflicts == []

    def test_one_day_cluster_yields_min_date(self):
        rows = [
            _xref_row('fb_a', 'fotmob', 'name_team', date(1998, 3, 2)),
            _xref_row('fb_a', 'sofascore', 'name_team', date(1998, 3, 1)),
        ]
        cmap, conflicts = xpr.build_canonical_dob_map(rows)
        assert cmap == {'fb_a': date(1998, 3, 1)}
        assert conflicts == []

    def test_disagreement_excludes_canonical_and_records_conflict(self):
        rows = [
            _xref_row('fb_a', 'fotmob', 'name_team', date(1998, 3, 1)),
            _xref_row('fb_a', 'transfermarkt', 'name_team', date(1995, 7, 20)),
        ]
        cmap, conflicts = xpr.build_canonical_dob_map(rows)
        assert cmap == {}
        assert len(conflicts) == 1
        assert conflicts[0]['canonical_id'] == 'fb_a'
        assert {s for s, _ in conflicts[0]['values']} == {'fotmob', 'transfermarkt'}

    def test_filters_fbref_orphans_weak_tiers_and_missing_dob(self):
        rows = [
            # FBref spine rows never contribute (they carry no Bronze DOB).
            _xref_row('fb_a', 'fbref', 'exact', date(1998, 3, 1)),
            # Weak fuzzy tier — excluded from the anchor set.
            _xref_row('fb_b', 'transfermarkt', 'name_team_surname',
                      date(1998, 3, 1)),
            # Orphans have no fb_ prefix.
            _xref_row('tm_9', 'transfermarkt', 'orphan', date(1998, 3, 1)),
            # Anchor tier but no DOB — no signal.
            _xref_row('fb_c', 'fotmob', 'name_team', None),
        ]
        cmap, conflicts = xpr.build_canonical_dob_map(rows)
        assert cmap == {}
        assert conflicts == []


# ---------------------------------------------------------------------------
# adjudicate_ambiguous_with_dob
# ---------------------------------------------------------------------------
class TestAdjudicate:
    AMB = {
        'rule': 'token_set_band',
        'candidates': [('g1', 'gabriel magalhaes', 100.0),
                       ('g2', 'gabriel jesus', 100.0),
                       ('g3', 'gabriel martinelli', 100.0)],
        'best_score': 100.0,
    }

    def test_unique_dob_match_promotes(self):
        cand = _candidate('transfermarkt', 't1', 'Gabriel', 'Arsenal',
                          dob=date(1997, 12, 19))
        cmap = {'fb_g1': date(1997, 12, 19)}
        got = xpr.adjudicate_ambiguous_with_dob(cand, self.AMB, cmap)
        assert got == ('fb_g1', 'name_team_dob', 100.0)

    def test_unknown_dob_candidates_do_not_block(self):
        cand = _candidate('transfermarkt', 't1', 'Gabriel', 'Arsenal',
                          dob=date(1997, 12, 19))
        # g2 / g3 have no consolidated DOB — must not block the unique match.
        cmap = {'fb_g1': date(1997, 12, 19), 'fb_other': date(1990, 1, 1)}
        got = xpr.adjudicate_ambiguous_with_dob(cand, self.AMB, cmap)
        assert got is not None and got[0] == 'fb_g1'

    def test_two_dob_matches_stay_ambiguous(self):
        cand = _candidate('transfermarkt', 't1', 'Gabriel', 'Arsenal',
                          dob=date(1997, 12, 19))
        cmap = {'fb_g1': date(1997, 12, 19), 'fb_g2': date(1997, 12, 19)}
        assert xpr.adjudicate_ambiguous_with_dob(cand, self.AMB, cmap) is None

    def test_no_dob_on_candidate_falls_through(self):
        cand = _candidate('transfermarkt', 't1', 'Gabriel', 'Arsenal')
        cmap = {'fb_g1': date(1997, 12, 19)}
        assert xpr.adjudicate_ambiguous_with_dob(cand, self.AMB, cmap) is None

    def test_tolerance_one_day_matches_two_days_does_not(self):
        cand = _candidate('transfermarkt', 't1', 'Gabriel', 'Arsenal',
                          dob=date(1997, 12, 20))
        assert xpr.adjudicate_ambiguous_with_dob(
            cand, self.AMB, {'fb_g1': date(1997, 12, 19)}
        ) is not None
        cand2 = _candidate('transfermarkt', 't1', 'Gabriel', 'Arsenal',
                           dob=date(1997, 12, 21))
        assert xpr.adjudicate_ambiguous_with_dob(
            cand2, self.AMB, {'fb_g1': date(1997, 12, 19)}
        ) is None


# ---------------------------------------------------------------------------
# _resolve_all end-to-end
# ---------------------------------------------------------------------------
class TestResolveAllDob:
    GABRIEL_SPINE = [
        _fb_row('g1', 'Gabriel Magalhaes', 'Arsenal'),
        _fb_row('g2', 'Gabriel Jesus', 'Arsenal'),
        _fb_row('g3', 'Gabriel Martinelli', 'Arsenal'),
    ]

    def test_namesake_bucket_split_via_dob(self):
        """#738: bare «Gabriel» is token_set-ambiguous across three real
        Gabriels; a DOB match against the canonical consolidated from an
        already-linked FotMob row must promote it with name_team_dob."""
        d1 = date(1997, 12, 19)
        fm = [_candidate('fotmob', 'f1', 'Gabriel Magalhaes', 'Arsenal',
                         dob=d1)]           # name_team anchor carrying DOB
        tm = [_candidate('transfermarkt', 't1', 'Gabriel', 'Arsenal',
                         dob=d1)]           # ambiguous without DOB
        rows, review, stats, dob_stats = _resolve(
            self.GABRIEL_SPINE, fm=fm, tm=tm)

        tm_rows = [r for r in rows if r['source'] == 'transfermarkt']
        assert len(tm_rows) == 1
        assert tm_rows[0]['canonical_id'] == 'fb_g1'
        assert tm_rows[0]['confidence'] == 'name_team_dob'
        assert stats['transfermarkt'] == {
            'total': 1, 'resolved': 1, 'orphan': 0, 'ambiguous': 0,
        }
        assert dob_stats['promoted_from_review'] == 1
        assert not [r for r in review if r['source'] == 'transfermarkt']

    def test_band_88_94_adjudicated_on_dob_match(self):
        """Single-candidate 88-94 token_set band + DOB match → promoted;
        match_score keeps the band score (name signal stays visible)."""
        d1 = date(1997, 8, 15)
        fb = [_fb_row('paq1', 'Lucas Paquette', 'Genoa')]
        fm = [_candidate('fotmob', 'f1', 'Lucas Paquette', 'Genoa', dob=d1)]
        us = [_candidate('understat', 'u_paq', 'Lucas Paqueta', 'Genoa',
                         dob=d1)]
        rows, review, stats, dob_stats = _resolve(fb, fm=fm, us=us)

        us_rows = [r for r in rows if r['source'] == 'understat']
        assert len(us_rows) == 1
        assert us_rows[0]['canonical_id'] == 'fb_paq1'
        assert us_rows[0]['confidence'] == 'name_team_dob'
        assert 88 <= us_rows[0]['match_score'] < 95
        assert dob_stats['promoted_from_review'] == 1
        assert review == []

    def test_band_dob_mismatch_stays_in_review(self):
        fb = [_fb_row('paq1', 'Lucas Paquette', 'Genoa')]
        fm = [_candidate('fotmob', 'f1', 'Lucas Paquette', 'Genoa',
                         dob=date(1997, 8, 15))]
        us = [_candidate('understat', 'u_paq', 'Lucas Paqueta', 'Genoa',
                         dob=date(1994, 1, 2))]
        rows, review, stats, dob_stats = _resolve(fb, fm=fm, us=us)
        assert not [r for r in rows if r['source'] == 'understat']
        assert len(review) == 1
        assert review[0]['rule'] == 'token_set_band'
        assert dob_stats['promoted_from_review'] == 0

    def test_dob_veto_demotes_fuzzy_link_to_review(self):
        """A surname-tier link whose DOB contradicts the canonical's
        consolidated DOB moves to review with rule='dob_veto'."""
        fb = [_fb_row('r1', 'Harrison Reed', 'Fulham')]
        fm = [_candidate('fotmob', 'f1', 'Harrison Reed', 'Fulham',
                         dob=date(1995, 1, 27))]      # anchor DOB
        # Different person: surname-tier match (last-token Levenshtein ≤1)
        # with a contradicting DOB.
        tm = [_candidate('transfermarkt', 't9', 'Bobby Reid', 'Fulham',
                         dob=date(1993, 2, 2))]
        rows, review, stats, dob_stats = _resolve(fb, fm=fm, tm=tm)

        assert not [r for r in rows if r['source'] == 'transfermarkt']
        veto_rows = [r for r in review if r['rule'] == 'dob_veto']
        assert len(veto_rows) == 1
        assert veto_rows[0]['source'] == 'transfermarkt'
        assert veto_rows[0]['candidates'] == ['r1']
        assert veto_rows[0]['candidate_names'] == ['Harrison Reed']
        assert stats['transfermarkt'] == {
            'total': 1, 'resolved': 0, 'orphan': 0, 'ambiguous': 1,
        }
        assert dob_stats['vetoed'] == 1

    def test_exact_and_alias_tiers_never_vetoed(self):
        """exact / name_team_alias links are authoritative — a contradicting
        DOB must NOT demote them (only surfaces via the dob_conflicts DQ)."""
        fb = [_fb_row('p1', 'Some Player', 'Arsenal')]
        fm = [_candidate('fotmob', 'f1', 'Some Player', 'Arsenal',
                         dob=date(1999, 9, 9))]
        # Exact tier: source_id equals the FBref player_id.
        us = [_candidate('understat', 'p1', 'Totally Different Name',
                         'Arsenal', dob=date(1980, 1, 1))]
        rows, review, stats, dob_stats = _resolve(fb, fm=fm, us=us)
        us_rows = [r for r in rows if r['source'] == 'understat']
        assert len(us_rows) == 1
        assert us_rows[0]['confidence'] == 'exact'
        assert dob_stats['vetoed'] == 0

    def test_no_dob_anywhere_is_a_strict_noop(self):
        """No ``dob`` keys → same rows/review/stats as the pre-DOB resolver."""
        fb = [
            _fb_row('p1', 'Some Player', 'Arsenal'),
            _fb_row('s1', 'Jose Smith', 'Chelsea'),
            _fb_row('s2', 'Jose Jones', 'Chelsea'),
        ]
        us = [
            _candidate('understat', 'u1', 'Some Player', 'Arsenal'),
            _candidate('understat', 'u2', 'Jose', 'Chelsea'),  # ambiguous
        ]
        rows, review, stats, dob_stats = _resolve(fb, us=us)

        assert stats['understat'] == {
            'total': 2, 'resolved': 1, 'orphan': 0, 'ambiguous': 1,
        }
        assert len(review) == 1
        assert review[0]['rule'] == 'token_set_band'
        assert not [r for r in rows if r['confidence'] == 'name_team_dob']
        assert dob_stats == {
            'canonical_dob_map': 0,
            'dob_conflicts': 0,
            'promoted_from_review': 0,
            'vetoed': 0,
            'conflicts': [],
        }

    def test_conflicting_anchor_dobs_disable_canonical(self):
        """Two anchor sources disagreeing on a canonical's DOB exclude it
        from the map — no veto, no promotion, conflict recorded."""
        fb = [_fb_row('p1', 'Some Player', 'Arsenal')]
        fm = [_candidate('fotmob', 'f1', 'Some Player', 'Arsenal',
                         dob=date(1999, 9, 9))]
        ss = [_candidate('sofascore', 's1', 'Some Player', 'Arsenal',
                         dob=date(1997, 1, 1))]
        rows, review, stats, dob_stats = _resolve(fb, fm=fm, ss=ss)
        assert dob_stats['dob_conflicts'] == 1
        assert dob_stats['canonical_dob_map'] == 0
        assert dob_stats['vetoed'] == 0
        # Both links survive (conflict is a report, not a demotion).
        assert stats['fotmob']['resolved'] == 1
        assert stats['sofascore']['resolved'] == 1


# ---------------------------------------------------------------------------
# _TIER_RANK / known pairs
# ---------------------------------------------------------------------------
class TestTierRankAndKnownPairs:
    def test_dob_tier_outranks_weak_fuzzy_tiers(self):
        r = xpr._TIER_RANK
        assert r['name_team_dob'] < r['name_team_nickname']
        assert r['name_team_dob'] < r['name_team_subset']
        assert r['name_team_dob'] < r['name_team_surname']
        assert r['exact'] < r['name_team'] < r['name_team_alias'] \
            < r['name_team_dob']

    def test_dob_row_owns_canonical_collision_over_surname(self):
        """TM canonical-collision: name_team_dob beats name_team_surname."""
        rows = [
            {'canonical_id': 'fb_x', 'source': 'transfermarkt',
             'source_id': '1', 'league': 'L', 'season': '2425',
             'confidence': 'name_team_dob', 'bronze_signal': -1.0},
            {'canonical_id': 'fb_x', 'source': 'transfermarkt',
             'source_id': '2', 'league': 'L', 'season': '2425',
             'confidence': 'name_team_surname', 'bronze_signal': -1.0},
        ]
        deduped, removed = xpr._dedup_canonical_per_season(rows)
        winners = [r for r in deduped if r['confidence'] == 'name_team_dob']
        assert len(winners) == 1 and winners[0]['source_id'] == '1'
        demoted = [r for r in deduped if r['confidence'] == 'orphan']
        assert len(demoted) == 1 and demoted[0]['source_id'] == '2'
        assert removed == {'transfermarkt': 1}

    def test_verify_known_pairs_required_sources_param(self):
        rows = []
        for _name, cid in xpr.KNOWN_PAIRS:
            rows.append({'canonical_id': cid, 'source': 'fbref'})
            rows.append({'canonical_id': cid, 'source': 'sofascore'})
            rows.append({'canonical_id': cid, 'source': 'fotmob'})
        # Default (core) set is not satisfied — no understat/whoscored rows.
        passed_core, total = xpr._verify_known_pairs(rows)
        assert (passed_core, total) == (0, len(xpr.KNOWN_PAIRS))
        # Extended set passes 10/10.
        passed_ext, _ = xpr._verify_known_pairs(
            rows, required_sources=xpr._KNOWN_PAIR_EXT_SOURCES)
        assert passed_ext == len(xpr.KNOWN_PAIRS)
