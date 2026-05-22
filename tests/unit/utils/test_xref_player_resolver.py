"""
Unit tests for ``dags/utils/xref_player_resolver.py`` (E1 T3).

Strategy
--------
* No Trino: :func:`run_resolver` is *not* exercised here — we validate every
  pure helper (``normalize_name``, ``fuzzy_match_score``, ``canonical_team_for_resolver``,
  ``cascade_resolve``) and the in-memory ``_FBrefSpine`` against fixture data.
* No mock framework: pure functions are easier to assert against directly,
  and the spine is small enough (5 fixtures) to construct in-line.
* Heavy deps (``rapidfuzz``, ``unidecode``) are imported lazily inside the
  module under test, so importing the module on the host has near-zero cost.

The known-pair regression check runs against a synthetic spine + candidate
list that mirrors the 10 hard-coded pairs — no Trino, no Bronze fixture.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


# Make `from utils.xref_player_resolver import ...` resolve on the host.
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / "dags"
for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)


# ---------------------------------------------------------------------------
# Module under test (imported once at test-collection time)
# ---------------------------------------------------------------------------
from utils import xref_player_resolver as xpr  # noqa: E402

# Skip the entire suite cleanly if rapidfuzz / unidecode aren't installed
# in the host venv (they're in the airflow image). No need to fail the
# whole pytest run for an env miss.
rapidfuzz = pytest.importorskip("rapidfuzz")
unidecode = pytest.importorskip("unidecode")


# ---------------------------------------------------------------------------
# normalize_name
# ---------------------------------------------------------------------------
class TestNormalizeName:
    def test_handles_none_and_empty(self):
        assert xpr.normalize_name(None) == ""
        assert xpr.normalize_name("") == ""
        assert xpr.normalize_name("   ") == ""

    def test_strips_diacritics(self):
        assert xpr.normalize_name("Joško Gvardiol") == "josko gvardiol"
        assert xpr.normalize_name("Bruno Guimarães") == "bruno guimaraes"
        assert xpr.normalize_name("Çalhanoğlu") == "calhanoglu"

    def test_lowercases_and_collapses_whitespace(self):
        assert xpr.normalize_name("Bukayo  Saka") == "bukayo saka"
        assert xpr.normalize_name("\tCole\nPalmer ") == "cole palmer"

    def test_preserves_token_order(self):
        # Order is preserved here — token-order-invariance is the job
        # of fuzzy_match_score, not normalize_name.
        assert xpr.normalize_name("Son Heung-min") == "son heung-min"
        assert xpr.normalize_name("Heung-Min Son") == "heung-min son"


# ---------------------------------------------------------------------------
# fuzzy_match_score
# ---------------------------------------------------------------------------
class TestFuzzyMatchScore:
    def test_token_order_invariance(self):
        # token_sort_ratio sorts tokens before comparing → 100 even with
        # different surface order.
        score = xpr.fuzzy_match_score("Son Heung-min", "Heung-Min Son")
        assert score >= 90.0, f"expected ≥90, got {score}"

    def test_diacritic_invariance(self):
        score = xpr.fuzzy_match_score("Joško Gvardiol", "Josko Gvardiol")
        assert score == 100.0

    def test_low_score_for_unrelated_names(self):
        score = xpr.fuzzy_match_score("Bukayo Saka", "Erling Haaland")
        assert score < xpr.NAME_THRESHOLD

    def test_empty_inputs_return_zero(self):
        assert xpr.fuzzy_match_score("", "Bukayo Saka") == 0.0
        assert xpr.fuzzy_match_score("Bukayo Saka", None) == 0.0
        assert xpr.fuzzy_match_score(None, None) == 0.0


# ---------------------------------------------------------------------------
# canonical_team_for_resolver — uses real medallion_config YAML
# (cheap: configs/medallion/team_aliases.yaml lives in the repo and the
# loader hits the disk once via lru_cache; no monkey-patching needed).
# ---------------------------------------------------------------------------
class TestCanonicalTeamForResolver:
    @classmethod
    def setup_class(cls):
        # Force MEDALLION_CONFIG_DIR to the real shipped configs so the
        # alias lookups exercise production data.
        import os
        os.environ.setdefault(
            "MEDALLION_CONFIG_DIR",
            str(PROJECT_ROOT / "configs" / "medallion"),
        )
        from utils import medallion_config
        medallion_config.CONFIG_DIR = Path(os.environ["MEDALLION_CONFIG_DIR"])
        medallion_config.reset_cache()

    def test_none_input_returns_none(self):
        assert xpr.canonical_team_for_resolver(None, "fbref") is None
        assert xpr.canonical_team_for_resolver("", "fbref") is None
        assert xpr.canonical_team_for_resolver("   ", "fbref") is None

    def test_known_alias_resolves(self):
        # 'Wolves' is a _generic alias for "Wolverhampton Wanderers".
        assert (
            xpr.canonical_team_for_resolver("Wolves", "matchhistory")
            == "Wolverhampton Wanderers"
        )

    def test_unmapped_passes_through_stripped(self):
        # Identity fallback: unknown clubs still produce SOMETHING so the
        # within-team fuzzy lookup can group rows that share a raw name.
        result = xpr.canonical_team_for_resolver("  Acme FC ", "fbref")
        assert result == "Acme FC"


# ---------------------------------------------------------------------------
# cascade_resolve
# ---------------------------------------------------------------------------
@pytest.fixture
def fbref_spine() -> "xpr._FBrefSpine":
    """Build a 5-player FBref spine fixture covering the cascade branches.

    All canonical_team values are *already* canonicalised (the spine is
    pure — alias YAML access happens upstream in the readers).
    """
    rows = [
        {
            'player_id': 'bc7dc64d',
            'source_id': 'bc7dc64d',
            'player_name': 'Bukayo Saka',
            'canonical_team': 'Arsenal',
            'season': '2425',
        },
        {
            'player_id': 'e342ad68',
            'source_id': 'e342ad68',
            'player_name': 'Mohamed Salah',
            'canonical_team': 'Liverpool',
            'season': '2425',
        },
        {
            'player_id': '5ad50391',
            'source_id': '5ad50391',
            'player_name': 'Joško Gvardiol',
            'canonical_team': 'Manchester City',
            'season': '2425',
        },
        {
            'player_id': '92e7e919',
            'source_id': '92e7e919',
            'player_name': 'Son Heung-min',
            'canonical_team': 'Tottenham Hotspur',
            'season': '2425',
        },
        {
            'player_id': 'e06683ca',
            'source_id': 'e06683ca',
            'player_name': 'Virgil van Dijk',
            'canonical_team': 'Liverpool',
            'season': '2425',
        },
    ]
    return xpr._FBrefSpine(rows)


class TestCascadeResolve:
    def test_exact_id_match(self, fbref_spine):
        # Source-id collision with FBref id -> 'exact', score=None
        cand = {
            'source': 'understat',
            'source_id': 'bc7dc64d',
            'player_name': 'Whatever',
            'canonical_team': 'Arsenal',
            'season': '2425',
        }
        cid, conf, score = xpr.cascade_resolve(cand, fbref_spine)
        assert cid == 'fb_bc7dc64d'
        assert conf == 'exact'
        assert score is None

    def test_name_team_match_diacritics(self, fbref_spine):
        # Understat strips diacritics — "Josko Gvardiol" must still resolve.
        cand = {
            'source': 'understat',
            'source_id': '12345',
            'player_name': 'Josko Gvardiol',
            'canonical_team': 'Manchester City',
            'season': '2425',
        }
        cid, conf, score = xpr.cascade_resolve(cand, fbref_spine)
        assert cid == 'fb_5ad50391'
        assert conf == 'name_team'
        assert score >= xpr.NAME_THRESHOLD

    def test_name_team_match_token_order(self, fbref_spine):
        # WhoScored sometimes flips order: "Heung-Min Son" must match
        # FBref's "Son Heung-min".
        cand = {
            'source': 'whoscored',
            'source_id': '777',
            'player_name': 'Heung-Min Son',
            'canonical_team': 'Tottenham Hotspur',
            'season': '2425',
        }
        cid, conf, score = xpr.cascade_resolve(cand, fbref_spine)
        assert cid == 'fb_92e7e919'
        assert conf == 'name_team'
        assert score >= xpr.NAME_THRESHOLD

    def test_team_mismatch_orphans(self, fbref_spine):
        # Right name, wrong team -> orphan (player on wrong team in source).
        cand = {
            'source': 'understat',
            'source_id': '999',
            'player_name': 'Bukayo Saka',
            'canonical_team': 'Liverpool',  # FBref Saka is at Arsenal
            'season': '2425',
        }
        cid, conf, score = xpr.cascade_resolve(cand, fbref_spine)
        assert cid == 'us_999'
        assert conf == 'orphan'

    def test_name_below_threshold_orphans(self, fbref_spine):
        # Same team, name nowhere close -> orphan.
        cand = {
            'source': 'whoscored',
            'source_id': '321',
            'player_name': 'Random Person',
            'canonical_team': 'Arsenal',
            'season': '2425',
        }
        cid, conf, score = xpr.cascade_resolve(cand, fbref_spine)
        assert cid == 'ws_321'
        assert conf == 'orphan'

    def test_unknown_source_raises_keyerror(self, fbref_spine):
        # Defensive: orphan-prefix lookup is hard-coded so a typo blows up
        # rather than silently emitting a malformed canonical_id.
        cand = {
            'source': 'tranfermarkt',  # not in resolver SOURCES
            'source_id': '1',
            'player_name': 'Random Person',
            'canonical_team': 'Arsenal',
            'season': '2425',
        }
        with pytest.raises(KeyError):
            xpr.cascade_resolve(cand, fbref_spine)


# ---------------------------------------------------------------------------
# Multi-season spine indexing (per-season buckets)
# ---------------------------------------------------------------------------
class TestMultiSeasonSpine:
    def test_player_in_multiple_season_buckets(self):
        # Cole Palmer: Man City 23-24, then Chelsea 24-25 — both buckets.
        rows = [
            {
                'player_id': 'abc12345',
                'source_id': 'abc12345',
                'player_name': 'Cole Palmer',
                'canonical_team': 'Manchester City',
                'season': '2324',
            },
            {
                'player_id': 'abc12345',
                'source_id': 'abc12345',
                'player_name': 'Cole Palmer',
                'canonical_team': 'Chelsea',
                'season': '2425',
            },
        ]
        spine = xpr._FBrefSpine(rows)
        assert ('2324', 'Manchester City') in spine.by_team
        assert ('2425', 'Chelsea') in spine.by_team

        # 24-25 Chelsea lookup hits the Chelsea bucket.
        cand_2425 = {
            'source': 'understat',
            'source_id': '2222',
            'player_name': 'Cole Palmer',
            'canonical_team': 'Chelsea',
            'season': '2425',
        }
        cid, conf, score = xpr.cascade_resolve(cand_2425, spine)
        assert cid == 'fb_abc12345'
        assert conf == 'name_team'
        assert score >= xpr.NAME_THRESHOLD

        # 23-24 Man City lookup hits the Man City bucket.
        cand_2324 = {
            'source': 'understat',
            'source_id': '1111',
            'player_name': 'Cole Palmer',
            'canonical_team': 'Manchester City',
            'season': '2324',
        }
        cid, conf, score = xpr.cascade_resolve(cand_2324, spine)
        assert cid == 'fb_abc12345'
        assert conf == 'name_team'
        assert score >= xpr.NAME_THRESHOLD

    def test_understat_seeking_other_season_bucket_is_orphan(self):
        # Player exists in 24-25 but not 25-26 — same-team lookup in
        # the missing season must NOT leak across into orphan match.
        rows = [
            {
                'player_id': 'def67890',
                'source_id': 'def67890',
                'player_name': 'Mads Hermansen',
                'canonical_team': 'Leicester City',
                'season': '2425',
            },
        ]
        spine = xpr._FBrefSpine(rows)
        cand = {
            'source': 'understat',
            'source_id': '999',
            'player_name': 'Mads Hermansen',
            'canonical_team': 'Leicester City',
            'season': '2526',
        }
        cid, conf, _ = xpr.cascade_resolve(cand, spine)
        assert cid == 'us_999'
        assert conf == 'orphan'

    def test_dedup_within_season(self):
        # Repeated enrichment must not double-add to the same bucket.
        rows = [
            {
                'player_id': 'ghi54321',
                'source_id': 'ghi54321',
                'player_name': 'Test Player',
                'canonical_team': 'Arsenal',
                'season': '2425',
            },
            {
                'player_id': 'ghi54321',
                'source_id': 'ghi54321',
                'player_name': 'Test Player',
                'canonical_team': 'Arsenal',
                'season': '2425',
            },
        ]
        spine = xpr._FBrefSpine(rows)
        assert len(spine.by_team[('2425', 'Arsenal')]) == 1

    def test_mid_season_transfer_within_same_season(self):
        # FBref Bronze stores two rows for a player who transferred mid-season
        # (e.g. Palmer 2023-24: Man City row + Chelsea row). Spine MUST place
        # the player in BOTH (season, team) buckets so candidates from either
        # source-side post-/pre-transfer team resolve correctly.
        rows = [
            {
                'player_id': 'pal12345',
                'source_id': 'pal12345',
                'player_name': 'Cole Palmer',
                'canonical_team': 'Manchester City',
                'season': '2324',
            },
            {
                'player_id': 'pal12345',
                'source_id': 'pal12345',
                'player_name': 'Cole Palmer',
                'canonical_team': 'Chelsea',
                'season': '2324',
            },
        ]
        spine = xpr._FBrefSpine(rows)
        assert ('2324', 'Manchester City') in spine.by_team
        assert ('2324', 'Chelsea') in spine.by_team
        # Understat side has only the post-transfer club — must still resolve.
        cand = {
            'source': 'understat',
            'source_id': 'u_palmer_2324',
            'player_name': 'Cole Palmer',
            'canonical_team': 'Chelsea',
            'season': '2324',
        }
        cid, conf, score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_pal12345'
        assert conf == 'name_team'
        assert score >= xpr.NAME_THRESHOLD

    def test_legacy_known_pairs_still_pass(self, fbref_spine):
        # Backward-compat regression guard: single-season fixture still
        # resolves a known pair (Joško Gvardiol @ Man City 24-25).
        cand = {
            'source': 'understat',
            'source_id': '12345',
            'player_name': 'Josko Gvardiol',
            'canonical_team': 'Manchester City',
            'season': '2425',
        }
        cid, conf, score = xpr.cascade_resolve(cand, fbref_spine)
        assert cid == 'fb_5ad50391'
        assert conf == 'name_team'
        assert score >= xpr.NAME_THRESHOLD

    def test_issue_15_saka_2526_understat_resolves(self):
        """Issue #15 regression: Saka 2025/26 understat must resolve via name_team.

        Reported symptom — silver.xref_player had no understat row for
        canonical_id='fb_bc7dc64d', season='2526'. Root cause was operational
        (stale xref relative to Bronze, not a cascade defect), but we lock the
        cascade contract here so a future regression of token_sort_ratio
        tuning or spine-bucket layout immediately fails the unit suite.
        """
        spine = xpr._FBrefSpine([
            {
                'player_id': 'bc7dc64d',
                'source_id': 'bc7dc64d',
                'player_name': 'Bukayo Saka',
                'canonical_team': 'Arsenal',
                'season': '2526',
            },
        ])
        cand = {
            'source': 'understat',
            'source_id': '7322',
            'player_name': 'Bukayo Saka',
            'canonical_team': 'Arsenal',
            'season': '2526',
        }
        cid, conf, score = xpr.cascade_resolve(cand, spine)
        assert cid == 'fb_bc7dc64d'
        assert conf == 'name_team'
        assert score >= xpr.NAME_THRESHOLD


# ---------------------------------------------------------------------------
# _verify_known_pairs (regression guard)
# ---------------------------------------------------------------------------
def _synthesize_pair_rows(pair_name: str, canonical_id: str):
    """Emit the 3 source rows that make a pair "pass" the known-pair gate."""
    return [
        {
            'canonical_id': canonical_id,
            'source': 'fbref',
            'source_id': canonical_id.removeprefix('fb_'),
        },
        {
            'canonical_id': canonical_id,
            'source': 'understat',
            'source_id': f'us_{pair_name.replace(" ", "_")}',
        },
        {
            'canonical_id': canonical_id,
            'source': 'whoscored',
            'source_id': f'ws_{pair_name.replace(" ", "_")}',
        },
    ]


class TestVerifyKnownPairs:
    def test_all_pairs_pass(self):
        rows = []
        for name, cid in xpr.KNOWN_PAIRS:
            rows.extend(_synthesize_pair_rows(name, cid))
        passed, total = xpr._verify_known_pairs(rows)
        assert passed == total == len(xpr.KNOWN_PAIRS)

    def test_partial_pass(self):
        rows = []
        # Only first 7 pairs get all 3 sources; remaining 3 are FBref-only.
        for i, (name, cid) in enumerate(xpr.KNOWN_PAIRS):
            if i < 7:
                rows.extend(_synthesize_pair_rows(name, cid))
            else:
                rows.append({
                    'canonical_id': cid, 'source': 'fbref',
                    'source_id': cid.removeprefix('fb_'),
                })
        passed, total = xpr._verify_known_pairs(rows)
        assert passed == 7
        assert total == 10

    def test_empty_rows_zero_passed(self):
        passed, total = xpr._verify_known_pairs([])
        assert passed == 0
        assert total == len(xpr.KNOWN_PAIRS)


# ---------------------------------------------------------------------------
# _resolve_all (end-to-end pure pipeline, no Trino)
# ---------------------------------------------------------------------------
class TestResolveAll:
    """Integration of cascade_resolve over multi-source fixture data.

    Goal: prove the resolver produces well-formed rows, correct stats, and
    that the regression guard fires when expected — without ever opening a
    Trino connection.
    """

    def _fb_row(self, player_id, name, team, season='2425'):
        return {
            'source': 'fbref',
            'player_id': player_id,
            'source_id': player_id,
            'player_name': name,
            'raw_team_name': team,
            'canonical_team': team,
            'league': 'ENG-Premier League',
            'season': season,
        }

    def _src_row(self, source, source_id, name, team, season='2425'):
        return {
            'source': source,
            'source_id': source_id,
            'player_name': name,
            'raw_team_name': team,
            'canonical_team': team,
            'league': 'ENG-Premier League',
            'season': season,
        }

    def test_end_to_end_minimal(self):
        fb = [
            self._fb_row('bc7dc64d', 'Bukayo Saka', 'Arsenal'),
            self._fb_row('5ad50391', 'Joško Gvardiol', 'Manchester City'),
        ]
        us = [
            # diacritic-stripped match
            self._src_row('understat', 'u1', 'Josko Gvardiol', 'Manchester City'),
            # orphan: unknown player
            self._src_row('understat', 'u2', 'Random Person', 'Arsenal'),
        ]
        ws = [
            # token-order match
            self._src_row('whoscored', '999', 'Saka Bukayo', 'Arsenal'),
        ]

        rows, review, stats = xpr._resolve_all(fb, us, ws, [])

        # Total rows = 2 fb + 2 us + 1 ws.
        assert len(rows) == 5
        # No ambiguity in this fixture (each name resolves uniquely).
        assert review == []

        # FBref rows are spine — all 'exact'.
        fb_rows = [r for r in rows if r['source'] == 'fbref']
        assert {r['canonical_id'] for r in fb_rows} == {
            'fb_bc7dc64d', 'fb_5ad50391'
        }
        assert all(r['confidence'] == 'exact' for r in fb_rows)

        # Understat: u1 resolves, u2 is orphan.
        us_rows = {r['source_id']: r for r in rows if r['source'] == 'understat'}
        assert us_rows['u1']['canonical_id'] == 'fb_5ad50391'
        assert us_rows['u1']['confidence'] == 'name_team'
        assert us_rows['u2']['canonical_id'] == 'us_u2'
        assert us_rows['u2']['confidence'] == 'orphan'

        # WhoScored: token-order Saka match
        ws_row = [r for r in rows if r['source'] == 'whoscored'][0]
        assert ws_row['canonical_id'] == 'fb_bc7dc64d'
        assert ws_row['confidence'] == 'name_team'

        # Stats ledger — v2 cascade tracks 'ambiguous' alongside 'orphan'.
        assert stats['fbref'] == {
            'total': 2, 'resolved': 2, 'orphan': 0, 'ambiguous': 0,
        }
        assert stats['understat'] == {
            'total': 2, 'resolved': 1, 'orphan': 1, 'ambiguous': 0,
        }
        assert stats['whoscored'] == {
            'total': 1, 'resolved': 1, 'orphan': 0, 'ambiguous': 0,
        }

    def test_orphan_prefix_per_source(self):
        fb = [self._fb_row('aaa', 'Some Name', 'Arsenal')]
        us = [self._src_row('understat', 'u_only', 'Nobody Match', 'Arsenal')]
        ws = [self._src_row('whoscored', 'w_only', 'Nobody Match', 'Arsenal')]
        rows, _review, _stats = xpr._resolve_all(fb, us, ws, [])
        orphans = {r['source']: r['canonical_id'] for r in rows
                   if r['confidence'] == 'orphan'}
        assert orphans == {
            'understat': 'us_u_only',
            'whoscored': 'ws_w_only',
        }


# ---------------------------------------------------------------------------
# Season helpers
# ---------------------------------------------------------------------------
class TestSeasonHelpers:
    def test_slug_to_fbref_year(self):
        assert xpr._slug_to_fbref_year(2425) == 2024
        assert xpr._slug_to_fbref_year(2122) == 2021
        assert xpr._slug_to_fbref_year(2526) == 2025
        with pytest.raises(ValueError):
            xpr._slug_to_fbref_year(99)  # too short

    def test_split_seasons_slug_to_fbref(self):
        # YAML emits slugs like 2425 -> FBref year-of-start 2024 +
        # legacy varchar '2425' for Understat/WhoScored.
        fbref, legacy = xpr._split_seasons([2425])
        assert fbref == [2024]
        assert legacy == ['2425']

        fbref, legacy = xpr._split_seasons([2122, 2425, 2526])
        assert fbref == [2021, 2024, 2025]
        assert legacy == ['2122', '2425', '2526']

    def test_seasons_in_clause_int(self):
        assert xpr._seasons_in_clause([2024]) == '2024'
        assert xpr._seasons_in_clause([2023, 2024]) == '2023, 2024'

    def test_seasons_in_clause_str(self):
        assert xpr._seasons_in_clause(['2425']) == "'2425'"

    def test_seasons_in_clause_rejects_quote_injection(self):
        with pytest.raises(ValueError):
            xpr._seasons_in_clause(["2425'; DROP TABLE x; --"])


# ---------------------------------------------------------------------------
# SQL escape helpers
# ---------------------------------------------------------------------------
class TestSqlHelpers:
    def test_sql_str_none(self):
        assert xpr._sql_str(None) == 'NULL'

    def test_sql_str_escapes_apostrophe(self):
        # "Nott'm Forest" must be rendered as 'Nott''m Forest'
        assert xpr._sql_str("Nott'm Forest") == "'Nott''m Forest'"

    def test_sql_double(self):
        assert xpr._sql_double(None) == 'NULL'
        assert xpr._sql_double(91.0) == 'CAST(91.0 AS DOUBLE)'

    def test_value_tuple_emits_well_formed(self):
        row = {
            'canonical_id': 'fb_xyz',
            'source': 'understat',
            'source_id': 'u1',
            'display_name': "O'Reilly",
            'league': 'ENG-Premier League',
            'season': '2425',
            'confidence': 'name_team',
            'match_score': 92.5,
            'raw_team_name': 'Spurs',
            'canonical_team': 'Tottenham Hotspur',
        }
        # Spot-check every field appears in the right order with proper escaping.
        out = xpr._value_tuple(row)
        assert out.startswith("('fb_xyz', 'understat', 'u1', 'O''Reilly', ")
        assert out.endswith(", 'Spurs', 'Tottenham Hotspur')")
        assert 'CAST(92.5 AS DOUBLE)' in out
