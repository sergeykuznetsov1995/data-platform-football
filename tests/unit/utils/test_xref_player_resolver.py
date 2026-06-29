"""
Unit tests for ``dags/utils/xref_player_resolver.py`` (E1 T3).

Strategy
--------
* No Trino: :func:`run_resolver` is *not* exercised here — we validate every
  pure helper (``normalize_name``, ``canonical_team_for_resolver``,
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
        # of the cascade's fuzzy matching, not normalize_name.
        assert xpr.normalize_name("Son Heung-min") == "son heung-min"
        assert xpr.normalize_name("Heung-Min Son") == "heung-min son"


# ---------------------------------------------------------------------------
# _is_youth_team — FotMob U21/U23 youth-squad detection (issue #563)
# ---------------------------------------------------------------------------
class TestIsYouthTeam:
    @pytest.mark.parametrize(
        "team",
        [
            "Arsenal U21",
            "Manchester City U23",
            "Chelsea U19",
            "West Ham United U21",
            "Sunderland U21",
            "Tottenham Under-21",
            "Crystal Palace U-21",
        ],
    )
    def test_youth_squads_match(self, team):
        assert xpr._is_youth_team(team) is True

    @pytest.mark.parametrize(
        "team",
        [
            "Arsenal",
            "Brighton & Hove Albion",
            "AFC Bournemouth",
            "Wolverhampton Wanderers",
            "Manchester City",
            "Luton Town",  # contains 'u' but no youth marker — must NOT match
        ],
    )
    def test_senior_teams_do_not_match(self, team):
        assert xpr._is_youth_team(team) is False

    def test_none_and_empty_are_not_youth(self):
        assert xpr._is_youth_team(None) is False
        assert xpr._is_youth_team("") is False


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
        # within-team fuzzy lookup can group rows that share a raw name. The
        # trailing FC/AFC is normalized away (#836), but grouping is preserved
        # (every "Acme FC" row maps to the same "Acme" bucket).
        result = xpr.canonical_team_for_resolver("  Acme FC ", "fbref")
        assert result == "Acme"

    def test_fc_suffix_normalized_to_spine_bucket(self):
        # #836: Transfermarkt's official "<name> FC" / "<name> AFC" names must
        # land in the same canonical bucket as the FBref spine ("<name>"), or the
        # whole roster orphans. The mapped canonical_name has no suffix.
        assert (
            xpr.canonical_team_for_resolver("Arsenal FC", "transfermarkt")
            == "Arsenal"
        )
        assert (
            xpr.canonical_team_for_resolver("Chelsea FC", "transfermarkt")
            == "Chelsea"
        )
        assert (
            xpr.canonical_team_for_resolver("Sunderland AFC", "transfermarkt")
            == "Sunderland"
        )

    def test_leading_afc_not_stripped(self):
        # "AFC Bournemouth" has AFC as a PREFIX — the $-anchored strip must not
        # touch it (it already maps correctly).
        assert (
            xpr.canonical_team_for_resolver("AFC Bournemouth", "transfermarkt")
            == "AFC Bournemouth"
        )


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

    def test_transfermarkt_capology_cascade(self):
        # FBref spine — Saka anchor.
        fb = [self._fb_row('bc7dc64d', 'Bukayo Saka', 'Arsenal')]
        # TM row: same name + team -> resolves to Saka.
        tm_match = self._src_row(
            'transfermarkt', '433177', 'Bukayo Saka', 'Arsenal',
        )
        # TM row: name doesn't match anything -> orphan with 'tm_' prefix.
        tm_orphan = self._src_row(
            'transfermarkt', '999999', 'Unknown Player', 'Arsenal',
        )
        # Capology row: matches Saka by name+team.
        cap_match = self._src_row(
            'capology', 'bukayo-saka', 'Bukayo Saka', 'Arsenal',
        )
        cap_orphan = self._src_row(
            'capology', 'unknown-slug', 'Random Guy', 'Arsenal',
        )

        rows, _review, stats = xpr._resolve_all(
            fb, [], [], [], None, [tm_match, tm_orphan], [cap_match, cap_orphan],
        )

        by_src = {(r['source'], r['source_id']): r for r in rows}

        # TM resolved + TM orphan
        assert by_src[('transfermarkt', '433177')]['canonical_id'] == 'fb_bc7dc64d'
        assert by_src[('transfermarkt', '433177')]['confidence'] == 'name_team'
        assert by_src[('transfermarkt', '999999')]['canonical_id'] == 'tm_999999'
        assert by_src[('transfermarkt', '999999')]['confidence'] == 'orphan'

        # Capology resolved + Capology orphan
        assert by_src[('capology', 'bukayo-saka')]['canonical_id'] == 'fb_bc7dc64d'
        assert by_src[('capology', 'bukayo-saka')]['confidence'] == 'name_team'
        assert by_src[('capology', 'unknown-slug')]['canonical_id'] == 'cap_unknown-slug'
        assert by_src[('capology', 'unknown-slug')]['confidence'] == 'orphan'

        # Stats ledger gained two new keys.
        assert stats['transfermarkt'] == {
            'total': 2, 'resolved': 1, 'orphan': 1, 'ambiguous': 0,
        }
        assert stats['capology'] == {
            'total': 2, 'resolved': 1, 'orphan': 1, 'ambiguous': 0,
        }

    def test_espn_cascade(self):
        """ESPN (#692): no native player_id → source_id is the ``player|team``
        composite built by ``_fetch_espn_players``. Rows cascade against the
        FBref spine exactly like the other sources; orphans carry 'es_'.
        """
        fb = [self._fb_row('bc7dc64d', 'Bukayo Saka', 'Arsenal')]
        # ESPN row: same name + team -> resolves to Saka.
        es_match = self._src_row(
            'espn', 'Bukayo Saka|Arsenal', 'Bukayo Saka', 'Arsenal',
        )
        # ESPN row: name unknown -> orphan with 'es_' prefix.
        es_orphan = self._src_row(
            'espn', 'Unknown Player|Arsenal', 'Unknown Player', 'Arsenal',
        )

        rows, _review, stats = xpr._resolve_all(
            fb, [], [], [], None, None, None, None, [es_match, es_orphan],
        )

        by_src = {(r['source'], r['source_id']): r for r in rows}
        assert by_src[('espn', 'Bukayo Saka|Arsenal')]['canonical_id'] == 'fb_bc7dc64d'
        assert by_src[('espn', 'Bukayo Saka|Arsenal')]['confidence'] == 'name_team'
        assert by_src[('espn', 'Unknown Player|Arsenal')]['canonical_id'] \
            == 'es_Unknown Player|Arsenal'
        assert by_src[('espn', 'Unknown Player|Arsenal')]['confidence'] == 'orphan'

        assert stats['espn'] == {
            'total': 2, 'resolved': 1, 'orphan': 1, 'ambiguous': 0,
        }

    def test_espn_namesakes_distinct_source_id(self):
        """ESPN namesakes on different clubs must NOT collide on the
        ``(source, source_id, league, season)`` PK — the ``player|team``
        composite keeps them distinct even when both orphan.
        """
        fb = [self._fb_row('bc7dc64d', 'Bukayo Saka', 'Arsenal')]
        es_a = self._src_row('espn', 'John Smith|Arsenal', 'John Smith', 'Arsenal')
        es_b = self._src_row('espn', 'John Smith|Chelsea', 'John Smith', 'Chelsea')

        rows, _review, _stats = xpr._resolve_all(
            fb, [], [], [], None, None, None, None, [es_a, es_b],
        )
        es_rows = [r for r in rows if r['source'] == 'espn']
        assert len(es_rows) == 2
        assert {r['source_id'] for r in es_rows} == {
            'John Smith|Arsenal', 'John Smith|Chelsea',
        }
        assert {r['canonical_id'] for r in es_rows} == {
            'es_John Smith|Arsenal', 'es_John Smith|Chelsea',
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
        # _silver_created_at lineage literal is the trailing 11th field (#374).
        assert out.endswith(", 'Spurs', 'Tottenham Hotspur', CURRENT_TIMESTAMP)")
        assert 'CAST(92.5 AS DOUBLE)' in out


# ---------------------------------------------------------------------------
# Issue #70 — _dedup_canonical_per_season
# ---------------------------------------------------------------------------
def _xrow(
    canonical_id: str,
    source: str,
    source_id: str,
    *,
    bronze_signal: float = -1.0,
    confidence: str = 'name_team',
    league: str = 'ENG-Premier League',
    season: str = '2526',
) -> dict:
    """Minimal xref-row fixture for dedup tests."""
    return {
        'canonical_id': canonical_id,
        'source': source,
        'source_id': source_id,
        'display_name': source_id,
        'league': league,
        'season': season,
        'confidence': confidence,
        'match_score': 95.0,
        'raw_team_name': 'Fulham',
        'canonical_team': 'fulham',
        'bronze_signal': bronze_signal,
    }


class TestDedupCanonicalPerSeason:
    def test_noop_when_one_row_per_group(self):
        rows = [
            _xrow('fb_a', 'understat', '910'),
            _xrow('fb_b', 'understat', '6827'),
            _xrow('fb_c', 'whoscored', '111'),
        ]
        out, removed = xpr._dedup_canonical_per_season(rows)
        assert len(out) == 3
        assert removed == {}

    def test_harrison_reed_pair_keeps_larger_bronze_signal(self):
        # Real-world #70 case: same canonical_id, 2 understat source_ids,
        # 6827 (primary club) has more minutes than 910 (legacy profile).
        old = _xrow('fb_harrison_reed', 'understat', '910', bronze_signal=120.0)
        new = _xrow('fb_harrison_reed', 'understat', '6827', bronze_signal=2100.0)
        out, removed = xpr._dedup_canonical_per_season([old, new])
        assert len(out) == 1
        assert out[0]['source_id'] == '6827'
        assert removed == {'understat': 1}

    def test_capology_tie_on_bronze_signal_falls_back_to_max_source_id(self):
        # When the signal proxy ties (e.g. both unavailable -1), the larger
        # numeric source_id wins. Capology uses the generic groups path — two
        # distinct source_ids on one canonical collapse to the winner (the
        # loser is dropped, since Capology has no TM-style demote path).
        a = _xrow('fb_x', 'capology', '111', bronze_signal=-1.0)
        b = _xrow('fb_x', 'capology', '999', bronze_signal=-1.0)
        out, removed = xpr._dedup_canonical_per_season([a, b])
        assert len(out) == 1
        assert out[0]['source_id'] == '999'
        assert removed == {'capology': 1}

    def test_orphans_pass_through_unchanged(self):
        # Orphan rows carry source-private canonical_ids (e.g. orphan:us:foo)
        # so collisions on (canonical, source) are NOT the fan-out pattern
        # this function targets. Keep them all.
        orphan_a = _xrow('orphan:us:foo', 'understat', '1', confidence='orphan')
        orphan_b = _xrow('orphan:us:bar', 'understat', '2', confidence='orphan')
        out, removed = xpr._dedup_canonical_per_season([orphan_a, orphan_b])
        assert len(out) == 2
        assert removed == {}

    def test_multi_source_collision_only_affects_offending_group(self):
        # Same canonical_id collides on understat (3 rows → 1) but FotMob
        # contributes a separate, lone row that must survive untouched.
        u1 = _xrow('fb_p', 'understat', '10', bronze_signal=50.0)
        u2 = _xrow('fb_p', 'understat', '20', bronze_signal=1000.0)
        u3 = _xrow('fb_p', 'understat', '30', bronze_signal=200.0)
        fm = _xrow('fb_p', 'fotmob', '777', bronze_signal=900.0)
        out, removed = xpr._dedup_canonical_per_season([u1, u2, u3, fm])
        assert len(out) == 2
        winners = {(r['source'], r['source_id']) for r in out}
        assert winners == {('understat', '20'), ('fotmob', '777')}
        assert removed == {'understat': 2}

    def test_different_seasons_keep_distinct_rows(self):
        # PK includes season — same canonical+source across two seasons must
        # produce two rows, no dedup.
        s1 = _xrow('fb_p', 'understat', '5', season='2425', bronze_signal=2000.0)
        s2 = _xrow('fb_p', 'understat', '5', season='2526', bronze_signal=2100.0)
        out, removed = xpr._dedup_canonical_per_season([s1, s2])
        assert len(out) == 2
        assert removed == {}

    def test_espn_transfer_rows_both_survive(self):
        # #720: ESPN source_id is '<name>|<team>'. A within-season transfer
        # resolves both club-stints to the SAME canonical_id. Collapsing them
        # would drop one club's row, and fct_lineup's ESPN JOIN (keyed on
        # raw_team_name) would NULL that club's player_id. Same-name stints stay.
        a = {**_xrow('fb_palmer', 'espn', 'Cole Palmer|Manchester City'),
             'raw_team_name': 'Manchester City'}
        b = {**_xrow('fb_palmer', 'espn', 'Cole Palmer|Chelsea'),
             'raw_team_name': 'Chelsea'}
        out, removed = xpr._dedup_canonical_per_season([a, b])
        assert len(out) == 2
        assert {r['raw_team_name'] for r in out} == {'Manchester City', 'Chelsea'}
        assert removed == {}

    def test_espn_namesake_false_match_demoted_to_orphan(self):
        # #803: surname tier binds a DIFFERENT player onto an existing canonical
        # on the thin historical spine (Steven Sessegnon → Ryan's fb_id). The
        # strongest-tier identity (Ryan, name_team) keeps the canonical; the
        # weaker (Steven, name_team_surname) is demoted to an es_<source_id>
        # orphan so it cannot pollute the real canonical in Gold.
        ryan = _xrow('fb_sessegnon', 'espn', 'Ryan Sessegnon|Fulham',
                     confidence='name_team', season='1819')
        steven = _xrow('fb_sessegnon', 'espn', 'Steven Sessegnon|Fulham',
                       confidence='name_team_surname', season='1819')
        out, removed = xpr._dedup_canonical_per_season([ryan, steven])
        assert len(out) == 2
        kept = [r for r in out if r['canonical_id'] == 'fb_sessegnon']
        demoted = [r for r in out if r['confidence'] == 'orphan']
        assert len(kept) == 1 and kept[0]['source_id'] == 'Ryan Sessegnon|Fulham'
        assert len(demoted) == 1
        assert demoted[0]['source_id'] == 'Steven Sessegnon|Fulham'
        assert demoted[0]['canonical_id'] == 'es_Steven Sessegnon|Fulham'
        assert removed == {'espn': 1}

    def test_espn_multiteam_owner_keeps_all_stints_demotes_namesake(self):
        # #803: combined case — Jonjo Shelvey legitimately at two clubs (#720)
        # PLUS George Shelvey false-matched via surname. Owner = Jonjo (name_team
        # beats name_team_surname); BOTH Jonjo club-stints survive, George only
        # is demoted to orphan.
        jonjo_a = _xrow('fb_shelvey', 'espn', 'Jonjo Shelvey|Nottingham Forest',
                        confidence='name_team', season='2223')
        jonjo_b = _xrow('fb_shelvey', 'espn', 'Jonjo Shelvey|Newcastle United',
                        confidence='name_team', season='2223')
        george = _xrow('fb_shelvey', 'espn', 'George Shelvey|Nottingham Forest',
                       confidence='name_team_surname', season='2223')
        out, removed = xpr._dedup_canonical_per_season([jonjo_a, jonjo_b, george])
        kept = {r['source_id'] for r in out if r['canonical_id'] == 'fb_shelvey'}
        assert kept == {'Jonjo Shelvey|Nottingham Forest',
                        'Jonjo Shelvey|Newcastle United'}
        demoted = [r for r in out if r['confidence'] == 'orphan']
        assert len(demoted) == 1 and demoted[0]['source_id'] == 'George Shelvey|Nottingham Forest'
        assert removed == {'espn': 1}

    def test_transfermarkt_collision_demotes_loser_to_orphan(self):
        # #788: TM source_id is a stable player_id — two DISTINCT source_ids on
        # one canonical/season is a false fuzzy-match on the thin historical
        # spine, NOT one player. Unlike the generic groups path (which drops the
        # loser), TM keeps the strongest-tier player on the canonical and demotes
        # the rest to tm_<source_id> orphans so they survive the table but cannot
        # fan-out the real canonical in Gold.
        a = _xrow('fb_x', 'transfermarkt', '111', confidence='name_team',
                  season='1819')
        b = _xrow('fb_x', 'transfermarkt', '999', confidence='name_team_surname',
                  season='1819')
        out, removed = xpr._dedup_canonical_per_season([a, b])
        assert len(out) == 2
        kept = [r for r in out if r['canonical_id'] == 'fb_x']
        demoted = [r for r in out if r['confidence'] == 'orphan']
        # Owner = strongest tier (name_team beats name_team_surname).
        assert len(kept) == 1 and kept[0]['source_id'] == '111'
        assert len(demoted) == 1
        assert demoted[0]['source_id'] == '999'
        assert demoted[0]['canonical_id'] == 'tm_999'
        assert removed == {'transfermarkt': 1}

    def test_transfermarkt_tier_tie_breaks_on_signal_then_source_id(self):
        # Same tier on both → fall back to _tie_key (bronze_signal, then numeric
        # source_id). TM signal is -1 for all, so the larger source_id owns the
        # canonical; the smaller is demoted to orphan (not dropped).
        a = _xrow('fb_y', 'transfermarkt', '111', confidence='name_team',
                  season='1920')
        b = _xrow('fb_y', 'transfermarkt', '999', confidence='name_team',
                  season='1920')
        out, removed = xpr._dedup_canonical_per_season([a, b])
        assert len(out) == 2
        kept = [r for r in out if r['canonical_id'] == 'fb_y']
        demoted = [r for r in out if r['confidence'] == 'orphan']
        assert kept[0]['source_id'] == '999'
        assert demoted[0]['source_id'] == '111'
        assert demoted[0]['canonical_id'] == 'tm_111'
        assert removed == {'transfermarkt': 1}

    def test_transfermarkt_same_player_id_club_stints_collapse_to_one(self):
        # A mid-season transfer gives the SAME TM player_id two anchor rows
        # (different raw_team). They resolve to the same canonical; the PK
        # (source, source_id, season) has no team, so they MUST collapse to one
        # row — and it is NOT a false match, so the canonical is kept (no orphan).
        a = {**_xrow('fb_z', 'transfermarkt', '555', confidence='name_team',
                     season='2425'),
             'raw_team_name': 'Manchester United'}
        b = {**_xrow('fb_z', 'transfermarkt', '555', confidence='name_team',
                     season='2425'),
             'raw_team_name': 'Aston Villa'}
        out, removed = xpr._dedup_canonical_per_season([a, b])
        assert len(out) == 1
        assert out[0]['source_id'] == '555'
        assert out[0]['canonical_id'] == 'fb_z'
        assert out[0]['confidence'] != 'orphan'
        assert removed == {'transfermarkt': 1}

    def test_transfermarkt_owner_keeps_canonical_demotes_two_namesakes(self):
        # Three distinct TM players false-matched onto one canonical: the
        # strongest tier (subset) owns it, the two weaker (surname) become
        # tm_<source_id> orphans.
        owner = _xrow('fb_w', 'transfermarkt', '100',
                      confidence='name_team_subset', season='1718')
        n1 = _xrow('fb_w', 'transfermarkt', '200',
                   confidence='name_team_surname', season='1718')
        n2 = _xrow('fb_w', 'transfermarkt', '300',
                   confidence='name_team_surname', season='1718')
        out, removed = xpr._dedup_canonical_per_season([owner, n1, n2])
        kept = {r['source_id'] for r in out if r['canonical_id'] == 'fb_w'}
        assert kept == {'100'}
        demoted = {r['source_id']: r['canonical_id']
                   for r in out if r['confidence'] == 'orphan'}
        assert demoted == {'200': 'tm_200', '300': 'tm_300'}
        assert removed == {'transfermarkt': 2}

    def test_transfermarkt_different_seasons_no_collision(self):
        # The collision-demote is per (canonical, season): the same two distinct
        # source_ids in DIFFERENT seasons are independent and both keep canonical.
        a = _xrow('fb_v', 'transfermarkt', '111', confidence='name_team',
                  season='2324')
        b = _xrow('fb_v', 'transfermarkt', '999', confidence='name_team',
                  season='2425')
        out, removed = xpr._dedup_canonical_per_season([a, b])
        assert len(out) == 2
        assert all(r['confidence'] != 'orphan' for r in out)
        assert removed == {}

    def test_transfermarkt_resolved_plus_orphan_stint_collapses_to_resolved(self):
        # #788 PK fix: a mid-season transfer gives ONE TM player_id two anchor
        # rows (different clubs). One stint resolves to FBref, the other orphans
        # (club not on the spine). The PK (source, source_id, league, season) has
        # no club, so they MUST collapse to one row — and a player found in ANY
        # club is resolved, so prefer the resolved stint (NOT a PK duplicate).
        resolved = {**_xrow('fb_p', 'transfermarkt', '243028',
                            confidence='name_team', season='2223'),
                    'raw_team_name': 'Aston Villa'}
        orphan = {**_xrow('tm_243028', 'transfermarkt', '243028',
                          confidence='orphan', season='2223'),
                  'raw_team_name': 'Southampton FC'}
        out, removed = xpr._dedup_canonical_per_season([resolved, orphan])
        assert len(out) == 1
        assert out[0]['source_id'] == '243028'
        assert out[0]['canonical_id'] == 'fb_p'
        assert out[0]['confidence'] == 'name_team'
        assert removed == {'transfermarkt': 1}

    def test_transfermarkt_all_orphan_stints_collapse_to_one_orphan(self):
        # Both club-stints of one player_id orphan (player on neither club's
        # spine) → exactly one orphan row survives (PK), not two.
        a = {**_xrow('tm_500', 'transfermarkt', '500', confidence='orphan',
                     season='1819'),
             'raw_team_name': 'Cardiff City'}
        b = {**_xrow('tm_500', 'transfermarkt', '500', confidence='orphan',
                     season='1819'),
             'raw_team_name': 'Huddersfield Town'}
        out, removed = xpr._dedup_canonical_per_season([a, b])
        assert len(out) == 1
        assert out[0]['source_id'] == '500'
        assert out[0]['confidence'] == 'orphan'
        assert removed == {'transfermarkt': 1}


# ---------------------------------------------------------------------------
# Silver lineage column (_silver_created_at) — charter §4 / S1 (issue #374)
# ---------------------------------------------------------------------------
class _RecordingCursor:
    """Captures executed SQL; returns an empty result set on fetchall()."""

    def __init__(self, sink):
        self._sink = sink

    def execute(self, sql):
        self._sink.append(sql)

    def fetchall(self):
        return []

    def close(self):
        pass


class _RecordingConn:
    def __init__(self):
        self.sql = []

    def cursor(self):
        return _RecordingCursor(self.sql)


class TestSilverLineageColumn:
    def test_value_tuple_appends_current_timestamp(self):
        # _silver_created_at must be the 11th VALUES field as a SQL literal,
        # so every materialised xref_player row carries lineage (charter §4).
        row = _xrow('fb_p', 'understat', '5')
        tup = xpr._value_tuple(row)
        assert tup.rstrip().endswith("CURRENT_TIMESTAMP)")

    def test_create_table_declares_lineage_column(self):
        conn = _RecordingConn()
        xpr._create_target_table(conn, 'iceberg.silver.xref_player')
        ddl = next(s for s in conn.sql if 'CREATE TABLE' in s)
        assert '_silver_created_at timestamp(6) with time zone' in ddl

    def test_insert_rows_lists_lineage_column(self):
        conn = _RecordingConn()
        xpr._insert_rows(conn, 'iceberg.silver.xref_player',
                         [_xrow('fb_p', 'understat', '5')], chunk_size=500)
        insert = next(s for s in conn.sql if s.startswith('INSERT INTO'))
        assert '_silver_created_at' in insert.split('VALUES')[0]
