"""
Medallion config loader (E1)
============================

Lightweight reader for ``configs/medallion/team_aliases.yaml`` and
``configs/medallion/competitions.yaml``. Used by:

  * T2 — pure-SQL ``silver.xref_team`` CTAS (consumes
    :func:`get_team_alias_sql_values` to embed the alias map as a
    Trino VALUES clause).
  * T3 — Python player resolver (consumes
    :func:`get_canonical_team_name` to canonicalise a team name before
    name-team-jersey similarity scoring).
  * Future DAG-level filters (consume :func:`get_in_scope_competitions`
    to skip stub competitions until E8a/b/c lights them up).

Design rules
------------
* No dependency on ``scrapers/*`` — importing this module MUST stay below
  ~50ms of import-time so Airflow DAG-parse cost is unaffected.
* Only ``yaml.safe_load`` (no PyYAML loaders that execute Python).
* Caching via :func:`functools.lru_cache` on module-level. Cache key is
  the resolved ``Path``, so unit tests can override ``CONFIG_DIR`` and
  call :func:`reset_cache` between runs.
* No Trino client lives here — pure IO + transformation.

Public API contract (frozen for T2/T3)
--------------------------------------
* :func:`load_team_aliases` -> dict
* :func:`load_competitions` -> dict
* :func:`get_team_alias_pairs(source, competition)` -> list[(raw, canonical)]
* :func:`get_canonical_team_name(raw_name, source)` -> str | None
* :func:`get_team_alias_sql_values(source)` -> str  (SQL VALUES body)
* :func:`get_in_scope_competitions` -> list[str]
* :func:`get_competition_seasons(competition_id)` -> list[int]
* :func:`render_sql_template(sql_path, **context)` -> str

The signatures above are the only thing T2/T3 may rely on. Internal
helpers (``_escape_sql_string``, ``_iter_team_aliases``) may change
without warning.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
# Default container path; override via env var for unit tests on the host.
# In the Airflow image, configs/ is bind-mounted at /opt/airflow/configs.
CONFIG_DIR = Path(
    os.environ.get('MEDALLION_CONFIG_DIR', '/opt/airflow/configs/medallion')
)

TEAM_ALIASES_FILE = 'team_aliases.yaml'
COMPETITIONS_FILE = 'competitions.yaml'
PLAYER_ALIASES_FILE = 'player_aliases.yaml'
REFEREE_ALIASES_FILE = 'referee_aliases.yaml'
MANAGER_ALIASES_FILE = 'manager_aliases.yaml'
VENUE_ALIASES_FILE = 'venue_aliases.yaml'
SOURCE_PRIORITY_FILE = 'source_priority.yaml'
COUNTRY_CODES_FILE = 'country_codes.yaml'

# Sentinel for "no source filter" — distinct from None which means "include
# generic + all sources merged" in get_team_alias_pairs. We pass the bucket
# name through to a single dict.get() so this constant just clarifies intent.
_GENERIC_BUCKET = '_generic'


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

class MedallionConfigError(ValueError):
    """Raised when team_aliases.yaml / competitions.yaml violate schema."""


def _validate_team_aliases_schema(doc: Dict) -> None:
    """Minimal structural validation. Does not enforce alias content."""
    if not isinstance(doc, dict) or 'teams' not in doc:
        raise MedallionConfigError(
            "team_aliases.yaml: missing top-level 'teams' key"
        )
    teams = doc['teams']
    if not isinstance(teams, list):
        raise MedallionConfigError(
            "team_aliases.yaml: 'teams' must be a list"
        )
    for i, t in enumerate(teams):
        if not isinstance(t, dict):
            raise MedallionConfigError(
                f"team_aliases.yaml: teams[{i}] must be a mapping"
            )
        if 'canonical_name' not in t:
            raise MedallionConfigError(
                f"team_aliases.yaml: teams[{i}] missing 'canonical_name'"
            )
        if 'aliases' not in t or not isinstance(t['aliases'], dict):
            raise MedallionConfigError(
                f"team_aliases.yaml: teams[{i}] ({t.get('canonical_name')}) "
                f"missing/invalid 'aliases'"
            )
        # canonical_id is the explicit identity slug (issue #141): identity must
        # NOT be derived from the display name. Enforce a stable, URL-safe slug.
        cid = t.get('canonical_id')
        if not isinstance(cid, str) or not re.fullmatch(r'[a-z0-9_]+', cid):
            raise MedallionConfigError(
                f"team_aliases.yaml: teams[{i}] ({t.get('canonical_name')}) "
                f"missing/invalid 'canonical_id' (must match ^[a-z0-9_]+$, "
                f"got {cid!r})"
            )
        # country / short_name feed gold.dim_team (issue #425) — non-empty
        # strings required, same contract as venue city/country.
        for field in ('country', 'short_name'):
            val = t.get(field)
            if not isinstance(val, str) or not val.strip():
                raise MedallionConfigError(
                    f"team_aliases.yaml: teams[{i}] ({t.get('canonical_name')}) "
                    f"missing/empty '{field}' (required for gold.dim_team)"
                )


def _validate_referee_aliases_schema(doc: Dict) -> None:
    """Structural validation of referee_aliases.yaml (issue #143).

    Mirror of :func:`_validate_team_aliases_schema` but keyed on ``referees``.
    Each entry needs ``canonical_name``, an ``aliases`` mapping, and an explicit
    ``canonical_id`` slug (``^[a-z0-9_]+$``) — identity is NOT derived from the
    raw name (FBref "Michael Oliver" vs MatchHistory "M Oliver" → one slug).
    """
    if not isinstance(doc, dict) or 'referees' not in doc:
        raise MedallionConfigError(
            "referee_aliases.yaml: missing top-level 'referees' key"
        )
    referees = doc['referees']
    if not isinstance(referees, list):
        raise MedallionConfigError(
            "referee_aliases.yaml: 'referees' must be a list"
        )
    seen_ids: set = set()
    for i, r in enumerate(referees):
        if not isinstance(r, dict):
            raise MedallionConfigError(
                f"referee_aliases.yaml: referees[{i}] must be a mapping"
            )
        if 'canonical_name' not in r:
            raise MedallionConfigError(
                f"referee_aliases.yaml: referees[{i}] missing 'canonical_name'"
            )
        if 'aliases' not in r or not isinstance(r['aliases'], dict):
            raise MedallionConfigError(
                f"referee_aliases.yaml: referees[{i}] ({r.get('canonical_name')}) "
                f"missing/invalid 'aliases'"
            )
        cid = r.get('canonical_id')
        if not isinstance(cid, str) or not re.fullmatch(r'[a-z0-9_]+', cid):
            raise MedallionConfigError(
                f"referee_aliases.yaml: referees[{i}] ({r.get('canonical_name')}) "
                f"missing/invalid 'canonical_id' (must match ^[a-z0-9_]+$, "
                f"got {cid!r})"
            )
        if cid in seen_ids:
            raise MedallionConfigError(
                f"referee_aliases.yaml: duplicate canonical_id {cid!r} "
                f"(referees[{i}]) — one slug must map to one referee"
            )
        seen_ids.add(cid)


def _validate_manager_aliases_schema(doc: Dict) -> None:
    """Structural validation of manager_aliases.yaml (issue #619).

    Mirror of :func:`_validate_referee_aliases_schema` but keyed on ``managers``.
    Each entry needs ``canonical_name``, an ``aliases`` mapping, and an explicit
    ``canonical_id`` slug. Unlike referees (``ref_<slug>``), the manager slug is
    the BARE FBref-spine name-normalize id (e.g. ``regis_le_bris``) so it joins
    straight onto the gold.dim_manager spine — NO ``mgr_`` prefix. The regex
    ``^[a-z0-9_]+$`` is identical; only the prefix convention differs.
    """
    if not isinstance(doc, dict) or 'managers' not in doc:
        raise MedallionConfigError(
            "manager_aliases.yaml: missing top-level 'managers' key"
        )
    managers = doc['managers']
    if not isinstance(managers, list):
        raise MedallionConfigError(
            "manager_aliases.yaml: 'managers' must be a list"
        )
    seen_ids: set = set()
    for i, m in enumerate(managers):
        if not isinstance(m, dict):
            raise MedallionConfigError(
                f"manager_aliases.yaml: managers[{i}] must be a mapping"
            )
        if 'canonical_name' not in m:
            raise MedallionConfigError(
                f"manager_aliases.yaml: managers[{i}] missing 'canonical_name'"
            )
        if 'aliases' not in m or not isinstance(m['aliases'], dict):
            raise MedallionConfigError(
                f"manager_aliases.yaml: managers[{i}] ({m.get('canonical_name')}) "
                f"missing/invalid 'aliases'"
            )
        cid = m.get('canonical_id')
        if not isinstance(cid, str) or not re.fullmatch(r'[a-z0-9_]+', cid):
            raise MedallionConfigError(
                f"manager_aliases.yaml: managers[{i}] ({m.get('canonical_name')}) "
                f"missing/invalid 'canonical_id' (must match ^[a-z0-9_]+$, "
                f"got {cid!r})"
            )
        if cid in seen_ids:
            raise MedallionConfigError(
                f"manager_aliases.yaml: duplicate canonical_id {cid!r} "
                f"(managers[{i}]) — one slug must map to one manager"
            )
        seen_ids.add(cid)


def _validate_venue_aliases_schema(doc: Dict) -> None:
    """Structural validation of venue_aliases.yaml (issue #145).

    Mirror of :func:`_validate_referee_aliases_schema` keyed on ``venues``.
    In addition to the curated-alias contract (``canonical_name``, ``aliases``
    mapping, unique ``canonical_id`` slug), venue entries MUST carry non-empty
    ``city`` and ``country`` strings — filling those is half the point of the
    refactor (namesake disambiguation + geocoding base).
    """
    if not isinstance(doc, dict) or 'venues' not in doc:
        raise MedallionConfigError(
            "venue_aliases.yaml: missing top-level 'venues' key"
        )
    venues = doc['venues']
    if not isinstance(venues, list):
        raise MedallionConfigError(
            "venue_aliases.yaml: 'venues' must be a list"
        )
    seen_ids: set = set()
    for i, v in enumerate(venues):
        if not isinstance(v, dict):
            raise MedallionConfigError(
                f"venue_aliases.yaml: venues[{i}] must be a mapping"
            )
        if 'canonical_name' not in v:
            raise MedallionConfigError(
                f"venue_aliases.yaml: venues[{i}] missing 'canonical_name'"
            )
        if 'aliases' not in v or not isinstance(v['aliases'], dict):
            raise MedallionConfigError(
                f"venue_aliases.yaml: venues[{i}] ({v.get('canonical_name')}) "
                f"missing/invalid 'aliases'"
            )
        cid = v.get('canonical_id')
        if not isinstance(cid, str) or not re.fullmatch(r'[a-z0-9_]+', cid):
            raise MedallionConfigError(
                f"venue_aliases.yaml: venues[{i}] ({v.get('canonical_name')}) "
                f"missing/invalid 'canonical_id' (must match ^[a-z0-9_]+$, "
                f"got {cid!r})"
            )
        if cid in seen_ids:
            raise MedallionConfigError(
                f"venue_aliases.yaml: duplicate canonical_id {cid!r} "
                f"(venues[{i}]) — one slug must map to one venue"
            )
        seen_ids.add(cid)
        for geo in ('city', 'country'):
            val = v.get(geo)
            if not isinstance(val, str) or not val.strip():
                raise MedallionConfigError(
                    f"venue_aliases.yaml: venues[{i}] "
                    f"({v.get('canonical_name')}) missing/empty {geo!r}"
                )
        # capacity (issue #434) is optional — absent => NULL in dim_venue. When
        # present it must be a positive int (bool is an int subclass — reject it).
        cap = v.get('capacity')
        if cap is not None and (not isinstance(cap, int) or isinstance(cap, bool) or cap <= 0):
            raise MedallionConfigError(
                f"venue_aliases.yaml: venues[{i}] "
                f"({v.get('canonical_name')}) invalid 'capacity' "
                f"(must be a positive int or omitted, got {cap!r})"
            )


def _validate_player_aliases_schema(doc: Dict) -> None:
    """Minimal structural validation of player_aliases.yaml.

    Allowed top-level shape::

        aliases: []                           # empty list — valid
        aliases:
          - source: understat
            source_id: "12345"
            fbref_player_id: "abcdef12"
            season: "2425" | "*"
            reason: "..."                     # optional

    Required fields per entry: ``source``, ``source_id``, ``fbref_player_id``,
    ``season``. ``reason`` is optional but encouraged. Unknown keys are
    permitted (forward-compatibility) but flagged via ValueError if the
    required keys are missing.
    """
    if not isinstance(doc, dict) or 'aliases' not in doc:
        raise MedallionConfigError(
            "player_aliases.yaml: missing top-level 'aliases' key"
        )
    aliases = doc['aliases']
    if aliases is None:
        return  # treat null as empty list — equivalent to []
    if not isinstance(aliases, list):
        raise MedallionConfigError(
            "player_aliases.yaml: 'aliases' must be a list"
        )
    required = ('source', 'source_id', 'fbref_player_id', 'season')
    for i, entry in enumerate(aliases):
        if not isinstance(entry, dict):
            raise MedallionConfigError(
                f"player_aliases.yaml: aliases[{i}] must be a mapping"
            )
        for key in required:
            if key not in entry:
                raise MedallionConfigError(
                    f"player_aliases.yaml: aliases[{i}] missing required "
                    f"key {key!r}"
                )
        # Source/source_id/fbref_player_id must be non-empty strings.
        for key in ('source', 'source_id', 'fbref_player_id'):
            val = entry[key]
            if not isinstance(val, str) or not val.strip():
                raise MedallionConfigError(
                    f"player_aliases.yaml: aliases[{i}].{key} "
                    f"must be a non-empty string (got {val!r})"
                )
        # season must be a 4-digit slug OR the literal "*"
        season_raw = entry['season']
        if not isinstance(season_raw, (str, int)):
            raise MedallionConfigError(
                f"player_aliases.yaml: aliases[{i}].season "
                f"must be a string slug or int (got {season_raw!r})"
            )
        season_str = str(season_raw)
        if season_str != '*' and not re.fullmatch(r'\d{4}', season_str):
            raise MedallionConfigError(
                f"player_aliases.yaml: aliases[{i}].season "
                f"must be 4-digit slug (e.g. '2425') or '*' "
                f"(got {season_str!r})"
            )


def _validate_competitions_schema(doc: Dict) -> None:
    if not isinstance(doc, dict) or 'competitions' not in doc:
        raise MedallionConfigError(
            "competitions.yaml: missing top-level 'competitions' key"
        )
    if not isinstance(doc['competitions'], list):
        raise MedallionConfigError(
            "competitions.yaml: 'competitions' must be a list"
        )
    for i, c in enumerate(doc['competitions']):
        if 'id' not in c:
            raise MedallionConfigError(
                f"competitions.yaml: competitions[{i}] missing 'id'"
            )
        if 'in_scope' not in c:
            raise MedallionConfigError(
                f"competitions.yaml: competitions[{i}] ({c.get('id')}) "
                f"missing 'in_scope' flag"
            )
        # country feeds gold.dim_competition (issue #425).
        country = c.get('country')
        if not isinstance(country, str) or not country.strip():
            raise MedallionConfigError(
                f"competitions.yaml: competitions[{i}] ({c.get('id')}) "
                f"missing/empty 'country' (required for gold.dim_competition)"
            )


def _validate_country_codes_schema(doc: Dict) -> None:
    """Structural validation of country_codes.yaml (issues #435, #585).

    Each entry maps a FBref/FIFA 3-letter ``code`` (``^[A-Z]{3}$``) to a
    non-empty full-country ``name``. Codes must be unique — a duplicate would
    silently fan out the country_map JOIN in gold.dim_player.

    #585: an entry MAY carry an optional ``aliases`` list of alternate source
    spellings that canonicalize to ``name``. Each alias must be a non-empty
    string, globally unique (one spelling -> one canonical), and must not
    collide with any canonical ``name`` (else a legitimately-canonical value
    would be remapped). The nationality_alias JOIN in gold.dim_player keys on
    the alias, so a duplicate alias would fan out the player spine.
    """
    if not isinstance(doc, dict) or 'countries' not in doc:
        raise MedallionConfigError(
            "country_codes.yaml: missing top-level 'countries' key"
        )
    countries = doc['countries']
    if not isinstance(countries, list):
        raise MedallionConfigError(
            "country_codes.yaml: 'countries' must be a list"
        )
    seen_codes: set = set()
    all_names: set = set()
    # (index, code, alias) — validated after the loop, once every canonical
    # name is known (the alias-vs-name collision guard needs the full set).
    alias_entries: List[tuple] = []
    for i, c in enumerate(countries):
        if not isinstance(c, dict):
            raise MedallionConfigError(
                f"country_codes.yaml: countries[{i}] must be a mapping"
            )
        code = c.get('code')
        if not isinstance(code, str) or not re.fullmatch(r'[A-Z]{3}', code):
            raise MedallionConfigError(
                f"country_codes.yaml: countries[{i}] has missing/invalid "
                f"'code' (must match ^[A-Z]{{3}}$, got {code!r})"
            )
        if code in seen_codes:
            raise MedallionConfigError(
                f"country_codes.yaml: duplicate code {code!r} "
                f"(countries[{i}]) — one code must map to one country"
            )
        seen_codes.add(code)
        name = c.get('name')
        if not isinstance(name, str) or not name.strip():
            raise MedallionConfigError(
                f"country_codes.yaml: countries[{i}] ({code}) "
                f"missing/empty 'name'"
            )
        all_names.add(name)
        aliases = c.get('aliases')
        if aliases is not None:
            if not isinstance(aliases, list):
                raise MedallionConfigError(
                    f"country_codes.yaml: countries[{i}] ({code}) "
                    f"'aliases' must be a list"
                )
            for a in aliases:
                if not isinstance(a, str) or not a.strip():
                    raise MedallionConfigError(
                        f"country_codes.yaml: countries[{i}] ({code}) "
                        f"has a missing/empty alias (got {a!r})"
                    )
                alias_entries.append((i, code, a))
    seen_aliases: set = set()
    for i, code, a in alias_entries:
        if a in all_names:
            raise MedallionConfigError(
                f"country_codes.yaml: alias {a!r} (countries[{i}], {code}) "
                f"collides with a canonical 'name' — drop it or fix the name"
            )
        if a in seen_aliases:
            raise MedallionConfigError(
                f"country_codes.yaml: duplicate alias {a!r} (countries[{i}], "
                f"{code}) — one spelling must map to one canonical name"
            )
        seen_aliases.add(a)


# ---------------------------------------------------------------------------
# YAML readers — cached on the resolved file path so tests overriding
# CONFIG_DIR get fresh reads after reset_cache().
# ---------------------------------------------------------------------------

@lru_cache(maxsize=8)
def _read_yaml(path: str) -> Dict:
    with open(path, 'r', encoding='utf-8') as fh:
        return yaml.safe_load(fh)


def reset_cache() -> None:
    """Clear all internal caches. Use in tests after monkey-patching paths."""
    _read_yaml.cache_clear()


def load_team_aliases() -> Dict:
    """Return the parsed team_aliases.yaml (with schema sanity-check)."""
    path = str(CONFIG_DIR / TEAM_ALIASES_FILE)
    doc = _read_yaml(path)
    _validate_team_aliases_schema(doc)
    return doc


def load_competitions() -> Dict:
    """Return the parsed competitions.yaml (with schema sanity-check)."""
    path = str(CONFIG_DIR / COMPETITIONS_FILE)
    doc = _read_yaml(path)
    _validate_competitions_schema(doc)
    return doc


def load_country_codes() -> Dict:
    """Return the parsed country_codes.yaml (issue #435, with schema check).

    Missing file is treated as ``{'countries': []}`` so a fresh checkout before
    #435 degrades to "no code mapping" (nationality keeps the raw FBref code)
    rather than crashing DAG-parse.
    """
    path = str(CONFIG_DIR / COUNTRY_CODES_FILE)
    if not Path(path).exists():
        return {'countries': []}
    doc = _read_yaml(path)
    _validate_country_codes_schema(doc)
    return doc


def load_referee_aliases() -> Dict:
    """Return the parsed referee_aliases.yaml (with schema sanity-check).

    Missing file is treated as ``{'referees': []}`` so an environment without
    the curated file (e.g. a fresh checkout before #143) degrades to "no
    referee aliases" rather than crashing DAG-parse.
    """
    path = str(CONFIG_DIR / REFEREE_ALIASES_FILE)
    if not Path(path).exists():
        return {'referees': []}
    doc = _read_yaml(path)
    _validate_referee_aliases_schema(doc)
    return doc


def load_manager_aliases() -> Dict:
    """Return the parsed manager_aliases.yaml (with schema sanity-check).

    Missing file is treated as ``{'managers': []}`` so an environment without
    the curated file (e.g. a fresh checkout before #619) degrades to "no
    manager aliases" — TM coaches fall back to plain name-normalize — rather
    than crashing DAG-parse.
    """
    path = str(CONFIG_DIR / MANAGER_ALIASES_FILE)
    if not Path(path).exists():
        return {'managers': []}
    doc = _read_yaml(path)
    _validate_manager_aliases_schema(doc)
    return doc


def load_venue_aliases() -> Dict:
    """Return the parsed venue_aliases.yaml (with schema sanity-check).

    Missing file is treated as ``{'venues': []}`` so a fresh checkout before
    #145 degrades to "no venue aliases" (every venue resolves to an orphan
    hash id) rather than crashing DAG-parse.
    """
    path = str(CONFIG_DIR / VENUE_ALIASES_FILE)
    if not Path(path).exists():
        return {'venues': []}
    doc = _read_yaml(path)
    _validate_venue_aliases_schema(doc)
    return doc


def load_player_aliases() -> Dict:
    """Return the parsed player_aliases.yaml (with schema sanity-check).

    Missing file is treated as ``{'aliases': []}`` — the v2 resolver tier-3
    is purely additive and an absent YAML file simply means "no overrides".
    """
    path = str(CONFIG_DIR / PLAYER_ALIASES_FILE)
    if not Path(path).exists():
        return {'aliases': []}
    doc = _read_yaml(path)
    _validate_player_aliases_schema(doc)
    # Normalise null aliases to empty list.
    if doc.get('aliases') is None:
        doc = {**doc, 'aliases': []}
    return doc


def _validate_source_priority_schema(doc: Dict) -> None:
    """Minimal structural validation for source_priority.yaml (issue #437).

    Each top-level key is a Gold table name; each maps output_alias -> spec,
    where spec has a non-empty ``sources`` list and every source carries an
    ``expr``. ``wrap`` (if present) must contain the ``{coalesce}`` marker.
    """
    if not isinstance(doc, dict):
        raise MedallionConfigError(
            "source_priority.yaml: top level must be a mapping of table -> metrics"
        )
    for table, metrics in doc.items():
        if not isinstance(metrics, dict):
            raise MedallionConfigError(
                f"source_priority.yaml: {table!r} must map metric -> spec"
            )
        for alias, spec in metrics.items():
            if not isinstance(spec, dict):
                raise MedallionConfigError(
                    f"source_priority.yaml: {table}.{alias} must be a mapping"
                )
            sources = spec.get('sources')
            if not isinstance(sources, list) or not sources:
                raise MedallionConfigError(
                    f"source_priority.yaml: {table}.{alias} needs a non-empty "
                    f"'sources' list"
                )
            for j, s in enumerate(sources):
                if not isinstance(s, dict) or not s.get('expr'):
                    raise MedallionConfigError(
                        f"source_priority.yaml: {table}.{alias}.sources[{j}] "
                        f"missing 'expr'"
                    )
            wrap = spec.get('wrap')
            if wrap is not None and '{coalesce}' not in wrap:
                raise MedallionConfigError(
                    f"source_priority.yaml: {table}.{alias} 'wrap' must contain "
                    f"the {{coalesce}} marker (got {wrap!r})"
                )


def load_source_priority() -> Dict:
    """Return the parsed source_priority.yaml (issue #437, with schema check).

    Missing file is treated as ``{}`` so an environment without the curated file
    degrades to "no templated facts" rather than crashing DAG-parse. The
    .sql.j2 render path raises loudly later if a table it needs is absent.
    """
    path = str(CONFIG_DIR / SOURCE_PRIORITY_FILE)
    if not Path(path).exists():
        return {}
    doc = _read_yaml(path)
    _validate_source_priority_schema(doc)
    return doc


# ---------------------------------------------------------------------------
# Team aliases — query helpers
# ---------------------------------------------------------------------------

def _team_in_scope(team: Dict, competition: Optional[str]) -> bool:
    """True if this team's competition_scope contains `competition`,
    OR if no competition filter was requested. Missing competition_scope
    is treated as "applies to ENG-Premier League" (default for E1)."""
    if competition is None:
        return True
    scope = team.get('competition_scope') or ['ENG-Premier League']
    return competition in scope


def _iter_team_aliases(
    source: Optional[str],
    competition: Optional[str],
    include_league: bool = False,
) -> List[Tuple[str, ...]]:
    """Yield (raw_name, canonical_name, canonical_id) tuples filtered by
    source/competition.

    Filter semantics:
      * source=None   -> UNION of `_generic` + every source-specific bucket.
      * source=<name> -> UNION of `_generic` + that bucket only.
        (Generic always included so canonical-name idempotency holds.)
      * competition=None -> all teams.
      * competition=X    -> teams whose competition_scope contains X.

    When ``include_league=True`` each alias is emitted once per competition in
    the team's ``competition_scope`` (defaulting to ``['ENG-Premier League']``)
    as a 4-tuple ``(raw, canonical, canonical_id, league)``. This lets the
    xref_team alias JOIN add an ``a.league = rt.league`` predicate (issue #148)
    so a bare short name disambiguates by league at worldwide scope. When
    ``include_league=False`` (default) the historical 3-tuple shape is kept.

    Deduplication: the same (raw, canonical) pair may appear in multiple
    buckets (e.g. _generic + matchhistory). We dedupe with an order-preserving
    seen-set so the output is stable for unit tests and SQL-diff review.
    """
    doc = load_team_aliases()
    seen: set = set()
    out: List[Tuple[str, ...]] = []
    for team in doc['teams']:
        if not _team_in_scope(team, competition):
            continue
        canonical = team['canonical_name']
        canonical_id = team['canonical_id']
        aliases = team.get('aliases') or {}
        # In league mode, fan each alias out over the team's scope. The [None]
        # sentinel keeps the non-league path at the historical 3-tuple shape.
        leagues = (
            (team.get('competition_scope') or ['ENG-Premier League'])
            if include_league else [None]
        )

        # Buckets to merge: _generic + (specific bucket OR every bucket).
        buckets: List[str] = [_GENERIC_BUCKET]
        if source is None:
            buckets.extend(k for k in aliases.keys() if k != _GENERIC_BUCKET)
        else:
            buckets.append(source)

        for bucket in buckets:
            for raw in aliases.get(bucket, []) or []:
                for league in leagues:
                    key = (
                        (raw, canonical, canonical_id)
                        if league is None
                        else (raw, canonical, canonical_id, league)
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(key)
    return out


def get_team_alias_pairs(
    source: Optional[str] = None,
    competition: Optional[str] = None,
) -> List[Tuple[str, str]]:
    """Return list of (raw_name, canonical_name) pairs.

    See :func:`_iter_team_aliases` for filter semantics.

    Empty list is a valid result (no team matches the filter); callers
    should treat empty as "skip alias-rewrite step" rather than as error.

    Note: ``_iter_team_aliases`` now yields 3-tuples carrying ``canonical_id``;
    this public helper keeps its historical ``(raw, canonical)`` contract.
    """
    return [(raw, canonical) for raw, canonical, _cid
            in _iter_team_aliases(source, competition)]


def get_canonical_team_name(
    raw_name: str,
    source: Optional[str] = None,
) -> Optional[str]:
    """Exact-match lookup raw_name -> canonical_name. Returns None if not found.

    Lookup is case-sensitive and exact: this matches the SQL ``=`` semantics
    of the legacy ``_team_aliases.sql`` so behaviour is bit-for-bit equivalent.
    Fuzzy matching belongs in T3 (player resolver) where it can score across
    multiple fields (name + team + jersey).
    """
    for raw, canonical, _cid in _iter_team_aliases(source, competition=None):
        if raw == raw_name:
            return canonical
    return None


# ---------------------------------------------------------------------------
# SQL emitters — used by T2 inline VALUES embedding
# ---------------------------------------------------------------------------

def _escape_sql_string(s: str) -> str:
    """Escape a string for embedding inside a single-quoted Trino literal.

    Doubles every apostrophe, refuses backslashes (Trino doesn't interpret
    them inside string literals but allowing them is a foot-gun if the SQL
    is later passed to a different engine), and rejects newlines / NULs to
    keep VALUES tuples on one line each (readable diffs, easier debugging).

    This is not a general-purpose SQL escaper — only good enough for the
    static, hand-curated team_aliases.yaml. Raises if the string contains
    a character that cannot be safely embedded.
    """
    if '\x00' in s or '\n' in s or '\r' in s:
        raise MedallionConfigError(
            f"team alias contains illegal whitespace/control char: {s!r}"
        )
    if '\\' in s:
        raise MedallionConfigError(
            f"team alias contains backslash (rejected): {s!r}"
        )
    # Defense-in-depth: even though apostrophe-doubling neutralises most
    # injection vectors, refuse SQL-comment / statement-terminator markers
    # outright. Same posture as `xref_dq.check_enum_compliance`.
    if ';' in s or '--' in s or '/*' in s or '*/' in s:
        raise MedallionConfigError(
            f"team alias contains forbidden SQL marker (;, --, /*, */): {s!r}"
        )
    return s.replace("'", "''")


def get_team_alias_sql_values(
    source: Optional[str] = None,
    competition: Optional[str] = None,
    with_canonical_id: bool = False,
    with_league: bool = False,
) -> str:
    """Render alias pairs as a Trino VALUES body for inline CTAS embedding.

    Output format (one tuple per line, indented by 4 spaces, no trailing
    comma on the last line, no leading/trailing whitespace on the result):

        ('Wolves', 'Wolverhampton Wanderers'),
        ('Spurs', 'Tottenham Hotspur'),
        ...
        ('Ipswich', 'Ipswich Town')

    When ``with_canonical_id=True`` each tuple gains the explicit identity slug
    (issue #141) so the consumer can resolve identity without re-deriving it
    from the display name:

        ('Wolves', 'Wolverhampton Wanderers', 'wolverhampton_wanderers'),
        ...

    When ``with_league=True`` each tuple additionally carries the league literal
    (issue #148) — emitted once per competition in the team's
    ``competition_scope`` — so the xref_team alias JOIN can guard on
    ``a.league = rt.league`` and disambiguate bare short names by league:

        ('Wolves', 'Wolverhampton Wanderers', 'wolverhampton_wanderers',
         'ENG-Premier League'),
        ...

    Intended use in T2 (xref_team.sql.j2):

        WITH aliases AS (
            SELECT raw_name, canonical_name, canonical_id, league FROM (VALUES
                {{ team_alias_values }}
            ) AS t(raw_name, canonical_name, canonical_id, league)
        )

    Apostrophes in raw names (e.g. ``Nott'm Forest``) are escaped to
    ``Nott''m Forest`` per ANSI SQL convention. Empty result raises —
    a CTAS over an empty VALUES is invalid Trino syntax, and an empty
    alias map almost certainly indicates a misconfigured filter.
    """
    rows = _iter_team_aliases(source, competition, include_league=with_league)
    if not rows:
        raise MedallionConfigError(
            f"get_team_alias_sql_values produced 0 pairs "
            f"(source={source!r}, competition={competition!r}); "
            "an empty VALUES clause is invalid Trino — refusing to emit."
        )
    if with_league:
        lines = [
            f"    ('{_escape_sql_string(raw)}', "
            f"'{_escape_sql_string(canonical)}', "
            f"'{_escape_sql_string(cid)}', "
            f"'{_escape_sql_string(league)}')"
            for raw, canonical, cid, league in rows
        ]
    elif with_canonical_id:
        lines = [
            f"    ('{_escape_sql_string(raw)}', "
            f"'{_escape_sql_string(canonical)}', "
            f"'{_escape_sql_string(cid)}')"
            for raw, canonical, cid in rows
        ]
    else:
        lines = [
            f"    ('{_escape_sql_string(raw)}', "
            f"'{_escape_sql_string(canonical)}')"
            for raw, canonical, _cid in rows
        ]
    return ',\n'.join(lines).lstrip()


# ---------------------------------------------------------------------------
# Referee aliases — query helpers (issue #143)
# ---------------------------------------------------------------------------

def _referee_in_scope(referee: Dict, competition: Optional[str]) -> bool:
    """True if this referee's competition_scope contains `competition`, OR if
    no competition filter was requested. Missing scope defaults to APL (E1)."""
    if competition is None:
        return True
    scope = referee.get('competition_scope') or ['ENG-Premier League']
    return competition in scope


def _iter_referee_aliases(
    source: Optional[str],
    competition: Optional[str],
    include_league: bool = False,
) -> List[Tuple[str, ...]]:
    """Yield (raw_name, canonical_name, canonical_id[, league]) tuples.

    Mirror of :func:`_iter_team_aliases` for ``referee_aliases.yaml``. Same
    bucket-merge (``_generic`` + source bucket), same league fan-out under
    ``include_league=True``, same order-preserving dedup.
    """
    doc = load_referee_aliases()
    seen: set = set()
    out: List[Tuple[str, ...]] = []
    for referee in doc.get('referees', []):
        if not _referee_in_scope(referee, competition):
            continue
        canonical = referee['canonical_name']
        canonical_id = referee['canonical_id']
        aliases = referee.get('aliases') or {}
        leagues = (
            (referee.get('competition_scope') or ['ENG-Premier League'])
            if include_league else [None]
        )

        buckets: List[str] = [_GENERIC_BUCKET]
        if source is None:
            buckets.extend(k for k in aliases.keys() if k != _GENERIC_BUCKET)
        else:
            buckets.append(source)

        for bucket in buckets:
            for raw in aliases.get(bucket, []) or []:
                for league in leagues:
                    key = (
                        (raw, canonical, canonical_id)
                        if league is None
                        else (raw, canonical, canonical_id, league)
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(key)
    return out


def get_referee_alias_sql_values(
    source: Optional[str] = None,
    competition: Optional[str] = None,
    with_canonical_id: bool = True,
    with_league: bool = True,
) -> str:
    """Render referee alias tuples as a Trino VALUES body (issue #143).

    Mirror of :func:`get_team_alias_sql_values`. Defaults emit the 4-tuple
    ``(raw_name, canonical_name, canonical_id, league)`` consumed by
    ``xref_referee.sql.j2``. Empty result raises (an empty VALUES is invalid
    Trino and almost certainly signals a missing/misfiltered config).
    """
    rows = _iter_referee_aliases(source, competition, include_league=with_league)
    if not rows:
        raise MedallionConfigError(
            f"get_referee_alias_sql_values produced 0 rows "
            f"(source={source!r}, competition={competition!r}); "
            "an empty VALUES clause is invalid Trino — refusing to emit."
        )
    if with_league:
        lines = [
            f"    ('{_escape_sql_string(raw)}', "
            f"'{_escape_sql_string(canonical)}', "
            f"'{_escape_sql_string(cid)}', "
            f"'{_escape_sql_string(league)}')"
            for raw, canonical, cid, league in rows
        ]
    elif with_canonical_id:
        lines = [
            f"    ('{_escape_sql_string(raw)}', "
            f"'{_escape_sql_string(canonical)}', "
            f"'{_escape_sql_string(cid)}')"
            for raw, canonical, cid in rows
        ]
    else:
        lines = [
            f"    ('{_escape_sql_string(raw)}', "
            f"'{_escape_sql_string(canonical)}')"
            for raw, canonical, _cid in rows
        ]
    return ',\n'.join(lines).lstrip()


# ---------------------------------------------------------------------------
# Manager aliases — query helpers (issue #619)
# ---------------------------------------------------------------------------

# Sentinel emitted when there are NO curated manager aliases, so the rendered
# VALUES clause stays valid Trino and the LEFT JOIN in
# transfermarkt_coaches.sql.j2 matches nothing — a graceful no-op (TM-coaches
# enrichment behaves exactly as pre-#619). This is a deliberate divergence from
# the referee/team emitters (which mandate aliases): a missing manager_aliases
# .yaml must NOT break the pre-existing coaches transform.
_NO_MANAGER_ALIAS_SENTINEL = '__no_manager_alias__'


def _manager_in_scope(manager: Dict, competition: Optional[str]) -> bool:
    """True if this manager's competition_scope contains `competition`, OR if
    no competition filter was requested. Missing scope defaults to APL."""
    if competition is None:
        return True
    scope = manager.get('competition_scope') or ['ENG-Premier League']
    return competition in scope


def _iter_manager_aliases(
    source: Optional[str],
    competition: Optional[str],
    include_league: bool = True,
) -> List[Tuple[str, ...]]:
    """Yield (raw_name, canonical_id[, league]) tuples from manager_aliases.yaml.

    Mirror of :func:`_iter_referee_aliases`, with two differences: the tuple
    drops ``canonical_name`` (TM silver keeps the source name as display) and
    ``canonical_id`` is the BARE FBref-spine id (e.g. ``regis_le_bris``) so it
    joins straight onto the gold.dim_manager spine. Same bucket-merge
    (``_generic`` + source bucket), league fan-out, and order-preserving dedup.
    """
    doc = load_manager_aliases()
    seen: set = set()
    out: List[Tuple[str, ...]] = []
    for manager in doc.get('managers', []):
        if not _manager_in_scope(manager, competition):
            continue
        canonical_id = manager['canonical_id']
        aliases = manager.get('aliases') or {}
        leagues = (
            (manager.get('competition_scope') or ['ENG-Premier League'])
            if include_league else [None]
        )

        buckets: List[str] = [_GENERIC_BUCKET]
        if source is None:
            buckets.extend(k for k in aliases.keys() if k != _GENERIC_BUCKET)
        else:
            buckets.append(source)

        for bucket in buckets:
            for raw in aliases.get(bucket, []) or []:
                for league in leagues:
                    key = (
                        (raw, canonical_id)
                        if league is None
                        else (raw, canonical_id, league)
                    )
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append(key)
    return out


def get_manager_alias_sql_values(
    source: Optional[str] = 'transfermarkt',
    competition: Optional[str] = None,
    with_league: bool = True,
) -> str:
    """Render manager alias tuples as a Trino VALUES body (issue #619).

    Default emits the 3-tuple ``(raw_name, canonical_id, league)`` consumed by
    ``transfermarkt_coaches.sql.j2`` (``source='transfermarkt'`` — the only
    consumer). Unlike :func:`get_referee_alias_sql_values`, an empty result is
    NOT an error: it emits a single guaranteed-non-matching sentinel row so the
    coaches transform keeps working (no-op) when no aliases are curated yet.
    """
    rows = _iter_manager_aliases(source, competition, include_league=with_league)
    if not rows:
        if with_league:
            return (
                f"    ('{_NO_MANAGER_ALIAS_SENTINEL}', "
                f"'{_NO_MANAGER_ALIAS_SENTINEL}', '__none__')"
            )
        return (
            f"    ('{_NO_MANAGER_ALIAS_SENTINEL}', "
            f"'{_NO_MANAGER_ALIAS_SENTINEL}')"
        )
    if with_league:
        lines = [
            f"    ('{_escape_sql_string(raw)}', "
            f"'{_escape_sql_string(cid)}', "
            f"'{_escape_sql_string(league)}')"
            for raw, cid, league in rows
        ]
    else:
        lines = [
            f"    ('{_escape_sql_string(raw)}', "
            f"'{_escape_sql_string(cid)}')"
            for raw, cid in rows
        ]
    return ',\n'.join(lines).lstrip()


# ---------------------------------------------------------------------------
# Venue aliases — query helpers (issue #145)
# ---------------------------------------------------------------------------

def _venue_in_scope(venue: Dict, competition: Optional[str]) -> bool:
    """True if this venue's competition_scope contains `competition`, OR if no
    competition filter was requested. Missing scope defaults to APL (E2)."""
    if competition is None:
        return True
    scope = venue.get('competition_scope') or ['ENG-Premier League']
    return competition in scope


def _iter_venue_aliases(
    source: Optional[str],
    competition: Optional[str],
) -> List[Tuple[str, str, str, str, str, str, Optional[int]]]:
    """Yield (raw_name, canonical_id, canonical_name, city, country, league, capacity).

    Mirror of :func:`_iter_referee_aliases`, but venues always carry city /
    country / league (no toggles — they are core to #145). The league is the
    venue's ``competition_scope`` fanned out one row per scope entry so the
    downstream JOIN can guard on ``a.league = u.league``.

    ``capacity`` (issue #434) is an optional curated stadium capacity (int) or
    ``None`` when unknown. It is constant per venue, so it rides on the tuple
    without affecting the dedup key.
    """
    doc = load_venue_aliases()
    seen: set = set()
    out: List[Tuple[str, str, str, str, str, str, Optional[int]]] = []
    for venue in doc.get('venues', []):
        if not _venue_in_scope(venue, competition):
            continue
        canonical = venue['canonical_name']
        canonical_id = venue['canonical_id']
        city = venue['city']
        country = venue['country']
        capacity = venue.get('capacity')
        aliases = venue.get('aliases') or {}
        leagues = venue.get('competition_scope') or ['ENG-Premier League']

        buckets: List[str] = [_GENERIC_BUCKET]
        if source is None:
            buckets.extend(k for k in aliases.keys() if k != _GENERIC_BUCKET)
        else:
            buckets.append(source)

        for bucket in buckets:
            for raw in aliases.get(bucket, []) or []:
                for league in leagues:
                    key = (raw, canonical_id, canonical, city, country, league)
                    if key in seen:
                        continue
                    seen.add(key)
                    out.append((*key, capacity))
    return out


def get_venue_alias_sql_values(
    source: Optional[str] = None,
    competition: Optional[str] = None,
    include_capacity: bool = False,
) -> str:
    """Render venue alias tuples as a Trino VALUES body (issue #145).

    Default: six-tuples
    ``(raw_name, canonical_id, canonical_name, city, country, league)`` —
    consumed by ``dim_match.sql.j2`` (venue_id resolution only).

    ``include_capacity=True`` (issue #434) appends a 7th column ``capacity``
    (integer literal or ``NULL``) for ``dim_venue.sql.j2``. capacity is emitted
    UNQUOTED — quoting it would make Trino read the column as varchar.

    Empty result raises (an empty VALUES is invalid Trino and almost certainly
    signals a missing/misfiltered config).
    """
    rows = _iter_venue_aliases(source, competition)
    if not rows:
        raise MedallionConfigError(
            f"get_venue_alias_sql_values produced 0 rows "
            f"(source={source!r}, competition={competition!r}); "
            "an empty VALUES clause is invalid Trino — refusing to emit."
        )
    if include_capacity:
        lines = [
            f"    ('{_escape_sql_string(raw)}', "
            f"'{_escape_sql_string(cid)}', "
            f"'{_escape_sql_string(canonical)}', "
            f"'{_escape_sql_string(city)}', "
            f"'{_escape_sql_string(country)}', "
            f"'{_escape_sql_string(league)}', "
            f"{'NULL' if cap is None else int(cap)})"
            for raw, cid, canonical, city, country, league, cap in rows
        ]
    else:
        lines = [
            f"    ('{_escape_sql_string(raw)}', "
            f"'{_escape_sql_string(cid)}', "
            f"'{_escape_sql_string(canonical)}', "
            f"'{_escape_sql_string(city)}', "
            f"'{_escape_sql_string(country)}', "
            f"'{_escape_sql_string(league)}')"
            for raw, cid, canonical, city, country, league, _cap in rows
        ]
    return ',\n'.join(lines).lstrip()


def get_team_meta_sql_values() -> str:
    """Render team metadata tuples as a Trino VALUES body (issue #425).

    Emits four-tuples ``(team_id, team_name, country, short_name)`` — one per
    club in team_aliases.yaml — consumed by ``dim_team.sql.j2``. Mirrors
    :func:`get_venue_alias_sql_values`; empty result raises because an empty
    VALUES clause is invalid Trino.
    """
    teams = load_team_aliases()['teams']
    if not teams:
        raise MedallionConfigError(
            "get_team_meta_sql_values produced 0 rows — "
            "an empty VALUES clause is invalid Trino, refusing to emit."
        )
    lines = [
        f"    ('{_escape_sql_string(t['canonical_id'])}', "
        f"'{_escape_sql_string(t['canonical_name'])}', "
        f"'{_escape_sql_string(t['country'])}', "
        f"'{_escape_sql_string(t['short_name'])}')"
        for t in teams
    ]
    return ',\n'.join(lines).lstrip()


def get_country_map_sql_values() -> str:
    """Render country-code tuples as a Trino VALUES body (issue #435).

    Emits two-tuples ``(fifa_code, country_name)`` — one per entry in
    country_codes.yaml — consumed by ``dim_player.sql.j2``. Mirrors
    :func:`get_team_meta_sql_values`; empty result raises because an empty
    VALUES clause is invalid Trino.
    """
    countries = load_country_codes()['countries']
    if not countries:
        raise MedallionConfigError(
            "get_country_map_sql_values produced 0 rows — "
            "an empty VALUES clause is invalid Trino, refusing to emit."
        )
    lines = [
        f"    ('{_escape_sql_string(c['code'])}', "
        f"'{_escape_sql_string(c['name'])}')"
        for c in countries
    ]
    return ',\n'.join(lines).lstrip()


def get_country_alias_sql_values() -> str:
    """Render source-spelling alias tuples as a Trino VALUES body (issue #585).

    Emits two-tuples ``(variant, canonical_name)`` — one per ``aliases`` entry
    in country_codes.yaml — consumed by ``dim_player.sql.j2`` to canonicalize
    the winning source nationality spelling (e.g. ``('USA', 'United States')``).
    Aliases are optional, so when none are configured a single no-op sentinel
    ``('', '')`` is emitted: an empty VALUES clause is invalid Trino, and the
    empty string never matches a real nationality.
    """
    countries = load_country_codes()['countries']
    lines = [
        f"    ('{_escape_sql_string(a)}', "
        f"'{_escape_sql_string(c['name'])}')"
        for c in countries
        for a in (c.get('aliases') or [])
    ]
    if not lines:
        lines = ["    ('', '')"]
    return ',\n'.join(lines).lstrip()


# ---------------------------------------------------------------------------
# Player aliases — query helper for the v2 resolver tier-3 fallback
# ---------------------------------------------------------------------------

def get_player_alias(
    source: str,
    source_id: str,
    season: str,
) -> Optional[str]:
    """Look up a hand-curated FBref player_id override for ``(source, source_id, season)``.

    The v2 resolver consults this AFTER all algorithmic tiers fail (surname,
    token_set, nicknames). Empty YAML / missing file is the common case at
    E1; this function returns ``None`` cleanly.

    Lookup precedence:
      1. Exact (source, source_id, season) match.
      2. (source, source_id, '*') wildcard match — used when an alias is
         valid across all configured seasons.

    Returns:
        FBref player_id WITHOUT the ``fb_`` prefix (caller prepends), or
        ``None`` if no entry matches.
    """
    if not source or not source_id:
        return None
    sid = str(source_id)
    season_str = str(season) if season is not None else ''
    doc = load_player_aliases()
    aliases = doc.get('aliases') or []

    exact: Optional[str] = None
    wildcard: Optional[str] = None
    for entry in aliases:
        if entry['source'] != source:
            continue
        if str(entry['source_id']) != sid:
            continue
        entry_season = str(entry['season'])
        if entry_season == season_str:
            exact = entry['fbref_player_id']
            break
        if entry_season == '*':
            wildcard = entry['fbref_player_id']
    return exact if exact is not None else wildcard


# ---------------------------------------------------------------------------
# Competitions — query helpers
# ---------------------------------------------------------------------------

def get_in_scope_competitions() -> List[str]:
    """Return ids of competitions where ``in_scope: true``.

    Used by DAGs to skip materialising stub competitions (E8a/b/c not yet
    rolled out). At E1 baseline this returns exactly ``['ENG-Premier League']``.
    """
    doc = load_competitions()
    return [c['id'] for c in doc['competitions'] if c.get('in_scope') is True]


def get_competition_seasons(competition_id: str) -> List[int]:
    """Return list of season ids (4-digit ints) for the given competition.

    Returns ``[]`` if the competition is a stub (no seasons configured).
    Raises ``KeyError`` if competition_id is not in the catalog at all,
    so DAGs surface typos loudly rather than silently materialising nothing.
    """
    doc = load_competitions()
    for c in doc['competitions']:
        if c['id'] == competition_id:
            return [s['id'] for s in (c.get('seasons') or [])]
    raise KeyError(f"competition not found in competitions.yaml: {competition_id!r}")


# ---------------------------------------------------------------------------
# SQL template rendering
# ---------------------------------------------------------------------------

# Matches a standalone-line ``{{ name }}`` placeholder. Mirrors the regex in
# dim_loaders._ROWS_PLACEHOLDER_RE so behaviour is consistent across modules.
_PLACEHOLDER_RE = re.compile(
    r'^[ \t]*\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}[ \t]*$',
    re.MULTILINE,
)


def render_sql_template(sql_path: Path, **context: str) -> str:
    """Render a Jinja-style SQL template by substituting `{{ name }}` placeholders.

    Supports the same single-line standalone placeholder syntax used by
    ``dags/sql/gold/dim_competition.sql.j2`` (one placeholder per line so
    line-based diffs stay readable). Multi-line / inline placeholders inside
    comments are intentionally NOT substituted to avoid wrecking SQL parsers
    when the docstring at the top of the template references the placeholder.

    Args:
        sql_path: absolute path to the template file.
        **context: name -> string substitutions. Each value MUST already be
            a valid SQL fragment — render_sql_template does no escaping.
            For team alias VALUES the caller should pass the output of
            :func:`get_team_alias_sql_values`.

    Returns:
        Rendered SQL as string.

    Raises:
        MedallionConfigError: if a placeholder in the template has no
            corresponding key in `context`. Unused context keys are allowed
            (forward-compat with templates that grow new placeholders).
    """
    template = Path(sql_path).read_text()
    missing: List[str] = []

    def _sub(match: 're.Match') -> str:
        name = match.group(1)
        if name not in context:
            missing.append(name)
            return match.group(0)
        # Preserve indentation of the matched line so VALUES bodies align.
        indent_match = re.match(r'^([ \t]*)', match.group(0))
        indent = indent_match.group(1) if indent_match else ''
        return indent + str(context[name])

    rendered = _PLACEHOLDER_RE.sub(_sub, template)
    if missing:
        raise MedallionConfigError(
            f"render_sql_template({sql_path}): missing context keys: "
            f"{sorted(set(missing))}"
        )
    return rendered


# ---------------------------------------------------------------------------
# Source-priority COALESCE emitter (issue #437)
# ---------------------------------------------------------------------------

def get_source_priority_exprs(table_name: str) -> Dict[str, str]:
    """Build the COALESCE SELECT-lines for a fact from source_priority.yaml.

    For each metric of ``table_name`` returns a ``{{ m_<alias> }}`` placeholder
    value: the full ``<wrap>COALESCE(expr1, expr2, ...) AS <alias>,`` SELECT
    line (trailing comma included — these are never the last SELECT item, see
    the .sql.j2 templates). Priority == order of ``sources`` in the YAML.

    The returned dict is passed straight to :func:`render_sql_template` as
    keyword context, so the keys mirror the template placeholders.

    Raises:
        MedallionConfigError: if the table has no entry in source_priority.yaml.
    """
    metrics = load_source_priority().get(table_name)
    if not metrics:
        raise MedallionConfigError(
            f"source_priority.yaml has no entry for table {table_name!r}"
        )
    out: Dict[str, str] = {}
    for alias, spec in metrics.items():
        exprs = [s['expr'] for s in spec['sources']]
        coalesce = f"COALESCE({', '.join(exprs)})"
        wrap = spec.get('wrap')
        body = wrap.replace('{coalesce}', coalesce) if wrap else coalesce
        out[f"m_{alias}"] = f"{body} AS {alias},"
    return out


def render_fact_sql(template_path, table_name: str) -> str:
    """Render a ``fct_*.sql.j2`` by filling its source-priority placeholders.

    Thin convenience wrapper: pulls the COALESCE lines for ``table_name`` from
    source_priority.yaml and substitutes them into the template. Returns the
    rendered SQL string (no file I/O on the output side — the caller decides
    whether to write a tempfile).
    """
    return render_sql_template(
        Path(template_path),
        **get_source_priority_exprs(table_name),
    )
