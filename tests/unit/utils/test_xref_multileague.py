"""
Multi-league readiness tests for the xref path (prep, not rollout).

Covers:
* ``KNOWN_PAIRS_BY_LEAGUE`` structure + the flat ``KNOWN_PAIRS`` back-compat
  alias.
* ``run_resolver`` skip-gate: a league with no curated anchors must NOT raise
  ``ResolverError`` — the gate is skipped with a WARNING and the summary says
  ``'skipped'``.
* ``write_mode`` validation (``'rebuild' | 'replace_league'``).
* Alias ``competition_scope`` fan-out: an alias scoped to another league must
  not emit tuples for the APL.

The resolver run is exercised with every Trino touchpoint monkeypatched to
in-memory stubs — no docker / Trino needed.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / "dags"
for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from utils import xref_player_resolver as xpr  # noqa: E402

rapidfuzz = pytest.importorskip("rapidfuzz")
unidecode = pytest.importorskip("unidecode")

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# KNOWN_PAIRS_BY_LEAGUE
# ---------------------------------------------------------------------------
class TestKnownPairsByLeague:
    def test_apl_anchors_present(self):
        assert 'ENG-Premier League' in xpr.KNOWN_PAIRS_BY_LEAGUE
        assert len(xpr.KNOWN_PAIRS_BY_LEAGUE['ENG-Premier League']) == 10

    def test_flat_alias_is_the_apl_set(self):
        """Back-compat: KNOWN_PAIRS is the APL entry (proto script + old
        tests refer to the flat tuple)."""
        assert xpr.KNOWN_PAIRS == xpr.KNOWN_PAIRS_BY_LEAGUE['ENG-Premier League']

    def test_verify_known_pairs_accepts_explicit_pairs(self):
        pairs = (('Some Player', 'fb_x1'),)
        rows = [
            {'canonical_id': 'fb_x1', 'source': s}
            for s in ('fbref', 'understat', 'whoscored')
        ]
        assert xpr._verify_known_pairs(rows, pairs=pairs) == (1, 1)
        # The default still evaluates the 10 APL anchors.
        assert xpr._verify_known_pairs(rows) == (0, 10)


# ---------------------------------------------------------------------------
# run_resolver: skip-gate + write_mode validation (Trino fully stubbed)
# ---------------------------------------------------------------------------
class _DummyConn:
    def close(self):
        pass


@pytest.fixture
def stubbed_resolver(monkeypatch):
    """Stub every Trino touchpoint of run_resolver to in-memory no-ops."""
    monkeypatch.setattr(xpr, '_get_trino_connection', lambda *a, **k: _DummyConn())
    for fetcher in (
        '_fetch_fbref_players', '_fetch_understat_players',
        '_fetch_whoscored_players', '_fetch_fotmob_players',
        '_fetch_sofascore_players', '_fetch_transfermarkt_players',
        '_fetch_capology_players', '_fetch_sofifa_players',
        '_fetch_espn_players',
    ):
        monkeypatch.setattr(xpr, fetcher, lambda *a, **k: [])
    monkeypatch.setattr(xpr, '_fetch_dob_maps', lambda *a, **k: {})
    monkeypatch.setattr(xpr, '_build_nicknamer', lambda: None)
    return xpr


class TestRunResolverMultiLeague:
    def test_unknown_league_skips_gate_instead_of_failing(self, stubbed_resolver):
        """A league with no KNOWN_PAIRS_BY_LEAGUE entry must not raise
        ResolverError — the gate is skipped and reported as 'skipped'."""
        summary = stubbed_resolver.run_resolver(
            league='XX-Test League',
            seasons=[2425],
            drop_before_insert=False,   # smoke mode — no Iceberg writes
        )
        assert summary['known_pair_pass_rate'] == 'skipped'
        assert summary['known_pair_pass_rate_ext'] == 'skipped'

    def test_apl_empty_rows_still_fails_the_gate(self, stubbed_resolver):
        """The APL gate semantics are unchanged: with anchors configured and
        zero resolved rows, the hard gate raises."""
        with pytest.raises(xpr.ResolverError):
            stubbed_resolver.run_resolver(
                league='ENG-Premier League',
                seasons=[2425],
                drop_before_insert=False,
            )

    def test_invalid_write_mode_rejected(self, stubbed_resolver):
        with pytest.raises(ValueError):
            stubbed_resolver.run_resolver(
                league='XX-Test League',
                seasons=[2425],
                drop_before_insert=False,
                write_mode='truncate',
            )


# ---------------------------------------------------------------------------
# competition_scope fan-out
# ---------------------------------------------------------------------------
_SCOPED_ALIASES = """\
managers:
  - canonical_name: "Xabi Alonso"
    canonical_id: "xabi_alonso"
    aliases:
      transfermarkt: ["Alonso, Xabi"]
    competition_scope: ["ESP-La Liga"]

  - canonical_name: "Mikel Arteta"
    canonical_id: "mikel_arteta"
    aliases:
      transfermarkt: ["Arteta, Mikel"]
    competition_scope: ["ENG-Premier League"]
"""


def test_alias_scope_fans_out_only_to_scoped_league(tmp_path, monkeypatch):
    """An alias scoped to ESP-La Liga must NOT emit an ENG-Premier League
    tuple — the league column in the VALUES body does the scoping, so the
    SQL-side `a.league = <src>.league` JOIN never fires cross-league."""
    (tmp_path / "manager_aliases.yaml").write_text(
        _SCOPED_ALIASES, encoding="utf-8"
    )
    from utils import medallion_config as mc
    monkeypatch.setattr(mc, "CONFIG_DIR", tmp_path)
    mc.reset_cache()
    try:
        values = mc.get_manager_alias_sql_values(source=None)
        assert "('Alonso, Xabi', 'xabi_alonso', 'ESP-La Liga')" in values
        assert "('Arteta, Mikel', 'mikel_arteta', 'ENG-Premier League')" in values
        # No cross-league leakage in either direction.
        assert "('Alonso, Xabi', 'xabi_alonso', 'ENG-Premier League')" not in values
        assert "('Arteta, Mikel', 'mikel_arteta', 'ESP-La Liga')" not in values
    finally:
        mc.reset_cache()
