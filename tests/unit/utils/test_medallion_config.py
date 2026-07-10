"""
Unit tests for ``dags/utils/medallion_config.py`` (E1 T1).

Strategy
--------
Tests run on the host (no Airflow / no Trino). We point ``MEDALLION_CONFIG_DIR``
at a tmp_path-managed fixture for the loader-mechanics tests, AND additionally
exercise the real shipped ``configs/medallion/*.yaml`` once at the bottom of
the file so a regression in the production YAML (missing club, syntax error)
trips immediately.

Apostrophe / SQL-injection coverage
-----------------------------------
The team alias map is hand-curated, but ``Nott'm Forest`` is a real APL alias
that contains an apostrophe. The loader MUST escape it as ``Nott''m Forest``
in the SQL VALUES output — otherwise the embedded CTAS in T2 explodes with
a SQL syntax error at the first run.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


# Make sure dags/ is on sys.path so `from utils.medallion_config import ...`
# resolves on the host. The dags-conftest does the same trick but is
# Airflow-specific; we replicate the parts we need here without pulling in
# the airflow stubs (this module has zero airflow imports).
PROJECT_ROOT = Path(__file__).resolve().parents[3]
DAGS_DIR = PROJECT_ROOT / "dags"
for p in (str(PROJECT_ROOT), str(DAGS_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

# A tiny, hand-curated team_aliases.yaml that covers all branches of the
# loader: _generic only, source-specific only, mixed, apostrophe handling,
# and a team out of competition_scope.
_MOCK_TEAM_ALIASES = """\
teams:
  - canonical_name: "Wolverhampton Wanderers"
    canonical_id: "wolverhampton_wanderers"
    country: "England"
    short_name: "Wolves"
    aliases:
      _generic: ["Wolves", "Wolverhampton Wanderers"]
      sofascore: ["Wolverhampton"]
      clubelo: ["WolverhamptonWanderers"]
    competition_scope: ["ENG-Premier League"]

  - canonical_name: "Tottenham Hotspur"
    canonical_id: "tottenham_hotspur"
    country: "England"
    short_name: "Spurs"
    aliases:
      _generic: ["Spurs", "Tottenham"]
      fbref: ["Tottenham Hotspur"]
    competition_scope: ["ENG-Premier League"]

  - canonical_name: "Manchester United"
    canonical_id: "manchester_united"
    country: "England"
    short_name: "Man Utd"
    aliases:
      _generic: ["Man Utd", "Manchester United"]
      clubelo: ["ManUnited"]
    competition_scope: ["ENG-Premier League"]

  - canonical_name: "Nottingham Forest"
    canonical_id: "nottingham_forest"
    country: "England"
    short_name: "Nott'm Forest"
    aliases:
      _generic: ["Nott'm Forest", "Nottingham Forest"]
    competition_scope: ["ENG-Premier League"]

  - canonical_name: "Newcastle United"
    canonical_id: "newcastle_united"
    country: "England"
    short_name: "Newcastle"
    aliases:
      _generic: ["Newcastle", "Newcastle Utd"]
    competition_scope: ["ENG-Premier League"]

  - canonical_name: "Real Madrid"
    canonical_id: "real_madrid"
    country: "Spain"
    short_name: "Real Madrid"
    aliases:
      _generic: ["Real Madrid"]
    competition_scope: ["ESP-La Liga"]
"""

_MOCK_COMPETITIONS = """\
competitions:
  - id: "ENG-Premier League"
    name: "English Premier League"
    country: "England"
    tier: 1
    seasons:
      - id: 2425
        format: "league_round_robin"
        team_count: 20
        start: "2024-08-16"
        end: "2025-05-25"
      - id: 2526
        format: "league_round_robin"
        team_count: 20
        start: "2025-08-15"
        end: "2026-05-24"
    sources:
      primary: ["fbref", "understat"]
      fallback: ["whoscored"]
    in_scope: true

  - id: "ESP-La Liga"
    name: "Spanish La Liga"
    country: "Spain"
    tier: 1
    seasons: []
    sources:
      primary: []
      fallback: []
    in_scope: false
    notes: "stub for E8b"
"""


@pytest.fixture
def mock_config_dir(tmp_path, monkeypatch):
    """Write mock YAML files to a tmp dir and point the loader at it.

    Resets the loader's lru_cache between tests so each test sees fresh
    file contents — otherwise a test that mutates the YAML would surprise
    subsequent tests via cache hits.
    """
    (tmp_path / "team_aliases.yaml").write_text(_MOCK_TEAM_ALIASES)
    (tmp_path / "competitions.yaml").write_text(_MOCK_COMPETITIONS)

    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    # Re-import to pick up new env var; alternatively rebind module-level
    # CONFIG_DIR. Re-importing is simpler and matches how the loader is
    # used in production (one process, env set at boot).
    import importlib

    from utils import medallion_config

    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    yield medallion_config

    # Reset for the next test (env var leaks via monkeypatch, but the
    # cached YAML reads do not).
    medallion_config.reset_cache()


# ---------------------------------------------------------------------------
# load_team_aliases — schema validation
# ---------------------------------------------------------------------------

def test_load_team_aliases_returns_dict(mock_config_dir):
    doc = mock_config_dir.load_team_aliases()
    assert isinstance(doc, dict)
    assert "teams" in doc
    assert len(doc["teams"]) == 6


def test_load_team_aliases_rejects_missing_top_level_key(tmp_path, monkeypatch):
    (tmp_path / "team_aliases.yaml").write_text("not_teams: []\n")
    (tmp_path / "competitions.yaml").write_text(_MOCK_COMPETITIONS)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    import importlib

    from utils import medallion_config

    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    with pytest.raises(medallion_config.MedallionConfigError, match="missing top-level"):
        medallion_config.load_team_aliases()


def test_load_team_aliases_rejects_team_without_canonical_name(tmp_path, monkeypatch):
    (tmp_path / "team_aliases.yaml").write_text(
        "teams:\n  - aliases:\n      _generic: ['x']\n"
    )
    (tmp_path / "competitions.yaml").write_text(_MOCK_COMPETITIONS)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    import importlib

    from utils import medallion_config

    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    with pytest.raises(medallion_config.MedallionConfigError, match="canonical_name"):
        medallion_config.load_team_aliases()


def test_load_team_aliases_rejects_team_without_aliases(tmp_path, monkeypatch):
    (tmp_path / "team_aliases.yaml").write_text(
        "teams:\n  - canonical_name: 'X'\n"
    )
    (tmp_path / "competitions.yaml").write_text(_MOCK_COMPETITIONS)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    import importlib

    from utils import medallion_config

    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    with pytest.raises(medallion_config.MedallionConfigError, match="aliases"):
        medallion_config.load_team_aliases()


def test_load_team_aliases_rejects_team_without_canonical_id(tmp_path, monkeypatch):
    # canonical_name + aliases present, but canonical_id missing (issue #141).
    (tmp_path / "team_aliases.yaml").write_text(
        "teams:\n"
        "  - canonical_name: 'X'\n"
        "    aliases:\n"
        "      _generic: ['X']\n"
    )
    (tmp_path / "competitions.yaml").write_text(_MOCK_COMPETITIONS)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    import importlib
    from utils import medallion_config
    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    with pytest.raises(medallion_config.MedallionConfigError, match="canonical_id"):
        medallion_config.load_team_aliases()


def test_load_team_aliases_rejects_invalid_canonical_id_slug(tmp_path, monkeypatch):
    # Uppercase / spaces / hyphens are not valid in a ^[a-z0-9_]+$ slug.
    (tmp_path / "team_aliases.yaml").write_text(
        "teams:\n"
        "  - canonical_name: 'X'\n"
        "    canonical_id: 'Bad-Slug'\n"
        "    aliases:\n"
        "      _generic: ['X']\n"
    )
    (tmp_path / "competitions.yaml").write_text(_MOCK_COMPETITIONS)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    import importlib
    from utils import medallion_config
    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    with pytest.raises(medallion_config.MedallionConfigError, match="canonical_id"):
        medallion_config.load_team_aliases()


def test_load_team_aliases_rejects_duplicate_canonical_id(tmp_path, monkeypatch):
    # Two teams sharing one slug (copy-paste error) must fail at load — same
    # guard the referee/manager/venue validators already have; without it the
    # dupe silently fans out gold.dim_team rows.
    (tmp_path / "team_aliases.yaml").write_text(
        "teams:\n"
        "  - canonical_name: 'X'\n"
        "    canonical_id: 'x_fc'\n"
        "    country: 'England'\n"
        "    short_name: 'X'\n"
        "    aliases:\n"
        "      _generic: ['X']\n"
        "    competition_scope: ['ENG-Premier League']\n"
        "  - canonical_name: 'Y'\n"
        "    canonical_id: 'x_fc'\n"
        "    country: 'England'\n"
        "    short_name: 'Y'\n"
        "    aliases:\n"
        "      _generic: ['Y']\n"
        "    competition_scope: ['ENG-Premier League']\n"
    )
    (tmp_path / "competitions.yaml").write_text(_MOCK_COMPETITIONS)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    import importlib
    from utils import medallion_config
    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    with pytest.raises(
        medallion_config.MedallionConfigError, match="duplicate canonical_id"
    ):
        medallion_config.load_team_aliases()


# ---------------------------------------------------------------------------
# get_team_alias_pairs
# ---------------------------------------------------------------------------

def test_alias_pairs_no_filter_returns_union_of_all_buckets(mock_config_dir):
    pairs = mock_config_dir.get_team_alias_pairs()
    # Must include _generic + every source-specific bucket. With our mock:
    # Wolves: _generic (2) + sofascore (1) + clubelo (1) = 4
    # Spurs:  _generic (2) + fbref (1) = 3
    # Man Utd: _generic (2) + clubelo (1) = 3
    # Nott'm: _generic (2) = 2
    # Newcastle: _generic (2) = 2
    # Real Madrid (no competition filter): _generic (1) = 1
    assert len(pairs) == 4 + 3 + 3 + 2 + 2 + 1

    # Every pair is (raw, canonical) tuple of strings.
    for raw, canonical in pairs:
        assert isinstance(raw, str)
        assert isinstance(canonical, str)


def test_alias_pairs_dedupes_when_same_pair_in_generic_and_source(tmp_path, monkeypatch):
    # Same value in both _generic and sofascore -> single output pair.
    (tmp_path / "team_aliases.yaml").write_text(
        "teams:\n"
        "  - canonical_name: 'X FC'\n"
        "    canonical_id: 'x_fc'\n"
        "    country: 'England'\n"
        "    short_name: 'X'\n"
        "    aliases:\n"
        "      _generic: ['X', 'X FC']\n"
        "      sofascore: ['X FC']\n"
        "    competition_scope: ['ENG-Premier League']\n"
    )
    (tmp_path / "competitions.yaml").write_text(_MOCK_COMPETITIONS)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    import importlib
    from utils import medallion_config
    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    pairs = medallion_config.get_team_alias_pairs()
    # Expect exactly 2 pairs: ('X','X FC') and ('X FC','X FC').
    assert sorted(pairs) == [('X', 'X FC'), ('X FC', 'X FC')]


def test_alias_pairs_source_filter_excludes_other_source_buckets(mock_config_dir):
    pairs = mock_config_dir.get_team_alias_pairs(source="sofascore")
    raw_names = [r for r, _ in pairs]
    # Sofascore-specific 'Wolverhampton' must appear; clubelo-specific
    # 'WolverhamptonWanderers' must NOT.
    assert "Wolverhampton" in raw_names
    assert "WolverhamptonWanderers" not in raw_names
    # Generic still included regardless of source filter.
    assert "Wolves" in raw_names
    # FBref-specific 'Tottenham Hotspur' must NOT appear (source=sofascore).
    assert "Tottenham Hotspur" not in raw_names


def test_alias_pairs_source_filter_clubelo(mock_config_dir):
    pairs = mock_config_dir.get_team_alias_pairs(source="clubelo")
    raw_names = [r for r, _ in pairs]
    assert "WolverhamptonWanderers" in raw_names
    assert "ManUnited" in raw_names
    # Sofascore-only 'Wolverhampton' must NOT be present.
    assert "Wolverhampton" not in raw_names


def test_alias_pairs_competition_filter_excludes_out_of_scope_team(mock_config_dir):
    pairs = mock_config_dir.get_team_alias_pairs(competition="ENG-Premier League")
    canonicals = {c for _, c in pairs}
    assert "Real Madrid" not in canonicals  # La Liga only
    assert "Wolverhampton Wanderers" in canonicals
    assert "Tottenham Hotspur" in canonicals


def test_alias_pairs_competition_filter_la_liga(mock_config_dir):
    pairs = mock_config_dir.get_team_alias_pairs(competition="ESP-La Liga")
    canonicals = {c for _, c in pairs}
    assert canonicals == {"Real Madrid"}


# ---------------------------------------------------------------------------
# get_canonical_team_name — corner-case APL aliases
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Wolves", "Wolverhampton Wanderers"),
        ("Wolverhampton", "Wolverhampton Wanderers"),
        ("Spurs", "Tottenham Hotspur"),
        ("Man Utd", "Manchester United"),
        ("Nott'm Forest", "Nottingham Forest"),  # apostrophe corner-case
        ("Newcastle", "Newcastle United"),
    ],
)
def test_canonical_lookup_apl_corner_cases(mock_config_dir, raw, expected):
    assert mock_config_dir.get_canonical_team_name(raw) == expected


def test_canonical_lookup_returns_none_for_unknown(mock_config_dir):
    assert mock_config_dir.get_canonical_team_name("Atletico Madrid") is None


def test_canonical_lookup_is_case_sensitive(mock_config_dir):
    # 'wolves' (lowercase) is intentionally not in the alias list — case
    # mismatches must surface as None, not silently resolve.
    assert mock_config_dir.get_canonical_team_name("wolves") is None


# ---------------------------------------------------------------------------
# get_team_alias_sql_values — apostrophe escaping + injection safety
# ---------------------------------------------------------------------------

def test_sql_values_format_is_indented_tuples(mock_config_dir):
    sql = mock_config_dir.get_team_alias_sql_values()
    # The first emitted line must NOT have leading whitespace (lstrip in
    # the output) so the {{ team_alias_values }} placeholder substitution
    # remains aligned with the surrounding template indent.
    assert not sql.startswith(" ")
    # Subsequent lines must be indented by 4 spaces.
    second_line = sql.split("\n", 1)[1]
    assert second_line.startswith("    (")
    # Every line ends with `)` or `),` — never a stray comma alone, never
    # a bare paren mismatch.
    for line in sql.splitlines():
        line = line.strip()
        assert line.startswith("(") and (line.endswith(")") or line.endswith("),"))


def test_sql_values_escapes_apostrophe_in_raw_name(mock_config_dir):
    sql = mock_config_dir.get_team_alias_sql_values()
    # The raw name 'Nott'm Forest' MUST appear escaped as 'Nott''m Forest'
    # inside the single-quoted SQL literal.
    assert "'Nott''m Forest'" in sql
    # The unescaped form is the explosion case — make sure we never emit it.
    # We check only the literal payload (between two `'`) so the substring
    # search is precise.
    assert "'Nott'm Forest'" not in sql


def test_sql_values_filtered_by_source(mock_config_dir):
    sql = mock_config_dir.get_team_alias_sql_values(source="clubelo")
    # ClubElo bucket has 'WolverhamptonWanderers' and 'ManUnited' — both
    # plus everything in _generic.
    assert "'WolverhamptonWanderers'" in sql
    assert "'ManUnited'" in sql
    # Sofascore-only 'Wolverhampton' must NOT be present in clubelo render.
    # Use boundary check to avoid matching 'WolverhamptonWanderers' substring.
    assert "'Wolverhampton'," not in sql
    assert "'Wolverhampton')" not in sql


def test_sql_values_with_canonical_id_emits_three_columns(mock_config_dir):
    # issue #141: xref_team renders 3-tuples carrying the explicit identity slug.
    sql = mock_config_dir.get_team_alias_sql_values(with_canonical_id=True)
    # The Wolves row must carry raw, canonical_name AND canonical_id.
    assert "('Wolves', 'Wolverhampton Wanderers', 'wolverhampton_wanderers')" in sql
    # Every tuple has exactly 3 comma-separated literals.
    for line in sql.splitlines():
        line = line.strip().rstrip(",").strip("()")
        assert line.count("',") + line.count("', ") >= 0  # sanity: parses
        # Count top-level literal separators: 3 quoted fields → 2 separators.
        assert line.count("', '") == 2, line


def test_sql_values_default_stays_two_columns(mock_config_dir):
    # Default (transfermarkt_transfers.sql.j2 contract) must be unchanged.
    sql = mock_config_dir.get_team_alias_sql_values()
    assert "('Wolves', 'Wolverhampton Wanderers')" in sql
    for line in sql.splitlines():
        line = line.strip().rstrip(",").strip("()")
        assert line.count("', '") == 1, line


def test_sql_values_with_league_emits_four_columns(mock_config_dir):
    # issue #148: xref_team renders 4-tuples carrying the league literal so the
    # alias JOIN can guard on `a.league = rt.league`.
    sql = mock_config_dir.get_team_alias_sql_values(with_league=True)
    # The Wolves row must carry raw, canonical_name, canonical_id AND league.
    assert (
        "('Wolves', 'Wolverhampton Wanderers', 'wolverhampton_wanderers', "
        "'ENG-Premier League')"
    ) in sql
    # Every tuple has exactly 4 comma-separated literals → 3 separators.
    for line in sql.splitlines():
        line = line.strip().rstrip(",").strip("()")
        assert line.count("', '") == 3, line


def test_sql_values_with_league_expands_per_competition(tmp_path, monkeypatch):
    # A team scoped to TWO competitions emits each alias once per league
    # (issue #148) — the league-tagged rows let the JOIN disambiguate a bare
    # short name (e.g. "Spartak") by league at worldwide scope.
    (tmp_path / "team_aliases.yaml").write_text(
        "teams:\n"
        "  - canonical_name: 'Spartak Moscow'\n"
        "    canonical_id: 'spartak_moscow'\n"
        "    country: 'Russia'\n"
        "    short_name: 'Spartak'\n"
        "    aliases:\n"
        "      _generic: ['Spartak']\n"
        "    competition_scope: ['RUS-Premier League', 'UEFA-Champions League']\n"
    )
    (tmp_path / "competitions.yaml").write_text(_MOCK_COMPETITIONS)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    import importlib
    from utils import medallion_config
    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    sql = medallion_config.get_team_alias_sql_values(with_league=True)
    rows = [line.strip().rstrip(",") for line in sql.splitlines()]
    # One alias × two leagues → exactly two rows, one per competition.
    assert rows == [
        "('Spartak', 'Spartak Moscow', 'spartak_moscow', 'RUS-Premier League')",
        "('Spartak', 'Spartak Moscow', 'spartak_moscow', 'UEFA-Champions League')",
    ]


def test_sql_values_raises_on_empty_result(mock_config_dir):
    # Filtering by an unknown source AND an unknown competition produces
    # zero rows — the loader refuses to emit invalid Trino syntax.
    with pytest.raises(mock_config_dir.MedallionConfigError, match="0 pairs"):
        mock_config_dir.get_team_alias_sql_values(competition="EUR-Champions League")  # unknown now that EUR fixed to UEFA; still 0 rows


def test_sql_values_rejects_backslash_in_alias(tmp_path, monkeypatch):
    (tmp_path / "team_aliases.yaml").write_text(
        "teams:\n"
        "  - canonical_name: 'X'\n"
        "    canonical_id: 'x'\n"
        "    country: 'England'\n"
        "    short_name: 'X'\n"
        "    aliases:\n"
        "      _generic: ['bad\\\\name']\n"
        "    competition_scope: ['ENG-Premier League']\n"
    )
    (tmp_path / "competitions.yaml").write_text(_MOCK_COMPETITIONS)
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(tmp_path))

    import importlib
    from utils import medallion_config
    importlib.reload(medallion_config)
    medallion_config.reset_cache()

    with pytest.raises(medallion_config.MedallionConfigError, match="backslash"):
        medallion_config.get_team_alias_sql_values()


# ---------------------------------------------------------------------------
# Competitions
# ---------------------------------------------------------------------------

def test_in_scope_competitions_returns_only_apl(mock_config_dir):
    assert mock_config_dir.get_in_scope_competitions() == ["ENG-Premier League"]


def test_competition_seasons_apl(mock_config_dir):
    seasons = mock_config_dir.get_competition_seasons("ENG-Premier League")
    assert seasons == [2425, 2526]


def test_competition_seasons_stub_returns_empty_list(mock_config_dir):
    # La Liga is a stub (in_scope=false, seasons=[]) — empty list, not error.
    assert mock_config_dir.get_competition_seasons("ESP-La Liga") == []


def test_competition_seasons_unknown_raises(mock_config_dir):
    # Typo / forgotten registration must surface loudly.
    with pytest.raises(KeyError, match="competition not found"):
        mock_config_dir.get_competition_seasons("XYZ-Wonderland")


# ---------------------------------------------------------------------------
# render_sql_template
# ---------------------------------------------------------------------------

def test_render_sql_template_substitutes_placeholder(tmp_path, mock_config_dir):
    template = tmp_path / "x.sql.j2"
    template.write_text(
        "SELECT * FROM (VALUES\n"
        "    {{ rows }}\n"
        ") AS t(a, b)\n"
    )
    out = mock_config_dir.render_sql_template(template, rows="('1', '2')")
    assert "('1', '2')" in out
    # Placeholder line is gone.
    assert "{{ rows }}" not in out


def test_render_sql_template_raises_on_missing_context(tmp_path, mock_config_dir):
    template = tmp_path / "x.sql.j2"
    template.write_text("SELECT\n    {{ rows }}\nFROM t\n")
    with pytest.raises(mock_config_dir.MedallionConfigError, match="missing context"):
        mock_config_dir.render_sql_template(template)


def test_render_sql_template_does_not_touch_inline_comment(tmp_path, mock_config_dir):
    # Inline ``-- {{ rows }} ...`` is non-standalone (other text on the line)
    # so the regex must skip it. This is the same gotcha dim_loaders solved.
    template = tmp_path / "x.sql.j2"
    template.write_text(
        "-- The {{ rows }} placeholder is described here.\n"
        "SELECT * FROM (VALUES\n"
        "    {{ rows }}\n"
        ") AS t(a, b)\n"
    )
    out = mock_config_dir.render_sql_template(template, rows="('x', 'y')")
    # Comment line is left alone (placeholder still present in the comment).
    assert "-- The {{ rows }} placeholder is described here." in out
    # Active VALUES line is substituted.
    assert "    ('x', 'y')" in out


# ---------------------------------------------------------------------------
# Smoke-test the actual shipped configs/medallion/*.yaml
# ---------------------------------------------------------------------------

@pytest.fixture
def real_config_dir(monkeypatch):
    """Point the loader at the real shipped configs/medallion/ dir."""
    real = PROJECT_ROOT / "configs" / "medallion"
    monkeypatch.setenv("MEDALLION_CONFIG_DIR", str(real))

    import importlib
    from utils import medallion_config
    importlib.reload(medallion_config)
    medallion_config.reset_cache()
    yield medallion_config
    medallion_config.reset_cache()


def test_real_config_loads_without_error(real_config_dir):
    teams = real_config_dir.load_team_aliases()
    competitions = real_config_dir.load_competitions()
    # Hand-curated APL: 31 clubs (20 current + 11 historical relegated).
    # We assert >= 30 to allow future additions without churning the test.
    assert len(teams["teams"]) >= 30
    assert len(competitions["competitions"]) >= 1


def test_real_config_in_scope_is_top5(real_config_dir):
    # E8b: Top-5 rollout flipped in_scope for the four new leagues. Each one
    # must carry a non-empty `seasons` list — an in_scope competition with
    # `seasons: []` silently drops its rows from dim_season (issue #425).
    assert set(real_config_dir.get_in_scope_competitions()) == {
        "ENG-Premier League",
        "ESP-La Liga",
        "ITA-Serie A",
        "GER-Bundesliga",
        "FRA-Ligue 1",
        "INT-World Cup",  # #913 Phase 2 (single_year)
    }
    for league in real_config_dir.get_in_scope_competitions():
        assert real_config_dir.get_competition_seasons(league), league


def test_real_config_apl_seasons_cover_ingested_history(real_config_dir):
    # #425: competitions.yaml covers the FULL ingested FBref history
    # (1617..2526) so dim_season satisfies the dim_match season FK.
    seasons = real_config_dir.get_competition_seasons("ENG-Premier League")
    assert len(seasons) == 10
    assert 1617 in seasons and 2425 in seasons and 2526 in seasons


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Wolves", "Wolverhampton Wanderers"),
        ("Spurs", "Tottenham Hotspur"),
        ("Man Utd", "Manchester United"),
        ("Nott'm Forest", "Nottingham Forest"),
        ("Newcastle", "Newcastle United"),
    ],
)
def test_real_config_canonical_corner_cases(real_config_dir, raw, expected):
    assert real_config_dir.get_canonical_team_name(raw) == expected


def test_real_config_sql_values_escapes_nott_apostrophe(real_config_dir):
    sql = real_config_dir.get_team_alias_sql_values()
    assert "'Nott''m Forest'" in sql
    # Unescaped form would break Trino — make sure we never emit it.
    assert "('Nott'm Forest'," not in sql


def test_real_config_sql_values_pair_count_matches_legacy_sql(real_config_dir):
    # The legacy _team_aliases.sql had 75 (raw, canonical) pairs.
    # Source-agnostic call (default) must produce ≥75 pairs (we may have
    # added MORE source-specific entries, but never fewer generics).
    pairs = real_config_dir.get_team_alias_pairs()
    assert len(pairs) >= 75


def test_real_config_savinho_alias_resolves(real_config_dir):
    # Issue #500: TM "Savinho" (743591) ↔ FBref "Sávio" (fe6e7156) nickname
    # bridge. season='*' wildcard must resolve for the live TM season (2526).
    assert real_config_dir.get_player_alias(
        "transfermarkt", "743591", "2526"
    ) == "fe6e7156"
