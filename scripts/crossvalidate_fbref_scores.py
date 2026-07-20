#!/usr/bin/env python3
"""
Cross-validate FBref match results against every other Bronze source (#892).

Why this exists
---------------
The #892 profile established that Bronze FBref is *internally consistent* — no
duplicates, no orphans, contract-clean. It never established that the numbers
are *true*. Only the EPL had ever been checked against another feed, and only
because `silver.xref_match` glues sources together there; the Top-5 leagues
have no team aliases yet (`configs/medallion/team_aliases.yaml` knows 35 EPL
clubs), so the xref spine cannot reach them.

This script sidesteps xref entirely. In a round-robin league the tuple
(league, season, home_team, away_team) identifies exactly one fixture, so two
feeds can be joined on it directly — no canonical ids, no alias table. Team
names are normalised (case-folded, diacritics stripped, non-alphanumerics
dropped) and matched exactly or by prefix containment, which handles
"Leganés"/"Leganes" and "Bayern Munich"/"Bayern" but keeps "Manchester United"
and "Manchester City" apart.

Reported per (source, league):
    fbref       fixtures with a final score in bronze.fbref_schedule
    matched     of those, joined to the other source
    agree       matched fixtures where both goal counts are identical
    mismatch    matched fixtures where they differ  <- the interesting bucket
    ambiguous   fbref fixtures matching >1 row in the other source (name collision)

`unmatched` (fbref - matched - ambiguous) is expected to be large: the other
feeds cover different season ranges. Mismatch, not unmatched, is the signal.

Read-only: SELECTs only, no writes.

Run inside the airflow container (so it can talk to Trino on the docker
network):

    docker compose exec airflow-scheduler \
        python /opt/airflow/scripts/crossvalidate_fbref_scores.py
    ... --source matchhistory --examples 20
"""

import argparse
import logging
import os
import sys
import warnings

warnings.filterwarnings('ignore', message='Unverified HTTPS request')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S',
)
logger = logging.getLogger('crossvalidate')

# FBref renders the score with an EN DASH (U+2013), not a hyphen.
EN_DASH = chr(8211)

# Case-fold, strip diacritics (NFD splits 'é' into 'e' + combining accent, and
# the character class then drops the accent), drop punctuation and spaces.
def _norm(expr: str) -> str:
    return f"regexp_replace(lower(normalize({expr}, NFD)), '[^a-z0-9]', '')"


# Each source projects to: league, season (slug), home, away, hg, ag.
# Season slugs: FBref/MatchHistory store the start year as a bigint; the rest
# already store the '1617' slug.
_YEAR_TO_SLUG = (
    "LPAD(CAST(MOD({c}, 100) AS varchar), 2, '0') || "
    "LPAD(CAST(MOD({c} + 1, 100) AS varchar), 2, '0')"
)

# A tie decided on penalties reads '(4) 1–(2) 1'; strip the shoot-out counts so
# only the 90-minute score remains — that is what every other feed stores.
_FBREF_SCORE = "regexp_replace(score, '\\(\\d+\\)\\s*', '')"

FBREF_SQL = f"""
    SELECT league,
           {_YEAR_TO_SLUG.format(c='season')} AS season,
           {_norm('home')} AS home,
           {_norm('away')} AS away,
           TRY_CAST(split_part({_FBREF_SCORE}, '{EN_DASH}', 1) AS integer) AS hg,
           TRY_CAST(split_part({_FBREF_SCORE}, '{EN_DASH}', 2) AS integer) AS ag
    FROM iceberg.bronze.fbref_schedule
    WHERE match_url IS NOT NULL AND score IS NOT NULL AND score <> ''
      -- #898: for an awarded match FBref publishes the forfeit score (Sassuolo 0–3
      -- Pescara) while every other feed keeps the on-pitch result (2–1). Comparing
      -- them is meaningless, so drop these 7 matches instead of reporting them.
      AND lower(coalesce(notes, '')) NOT LIKE 'match awarded to%'
"""

SOURCES = {
    'matchhistory': f"""
        SELECT league,
               {_YEAR_TO_SLUG.format(c='season')} AS season,
               {_norm('home_team')} AS home,
               {_norm('away_team')} AS away,
               TRY_CAST(home_goals AS integer) AS hg,
               TRY_CAST(away_goals AS integer) AS ag
        FROM iceberg.bronze.matchhistory_results
        WHERE home_goals IS NOT NULL AND away_goals IS NOT NULL
    """,
    'whoscored': f"""
        SELECT league, season,
               {_norm('home_team')} AS home,
               {_norm('away_team')} AS away,
               TRY_CAST(home_score AS integer) AS hg,
               TRY_CAST(away_score AS integer) AS ag
        FROM iceberg.bronze.whoscored_schedule
        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
    """,
    'sofascore': f"""
        SELECT league, season,
               {_norm('home_team')} AS home,
               {_norm('away_team')} AS away,
               TRY_CAST(home_score AS integer) AS hg,
               TRY_CAST(away_score AS integer) AS ag
        FROM iceberg.bronze.sofascore_schedule
        WHERE home_score IS NOT NULL AND away_score IS NOT NULL
    """,
    'espn': f"""
        SELECT league, season,
               {_norm('home_team')} AS home,
               {_norm('away_team')} AS away,
               TRY_CAST(home_goals AS integer) AS hg,
               TRY_CAST(away_goals AS integer) AS ag
        FROM iceberg.bronze.espn_schedule
        WHERE status = 'STATUS_FULL_TIME'
    """,
    'understat': f"""
        SELECT league, season,
               {_norm('home_team')} AS home,
               {_norm('away_team')} AS away,
               TRY_CAST(home_goals AS integer) AS hg,
               TRY_CAST(away_goals AS integer) AS ag
        FROM iceberg.bronze.understat_schedule
        WHERE is_result = true
    """,
}

# Exact normalised name, or one a prefix of the other ("bayernmunich"/"bayern").
# Prefix containment keeps "manchesterunited" and "manchestercity" distinct.
_TEAM_MATCH = (
    "(f.{s} = o.{s} OR strpos(f.{s}, o.{s}) = 1 OR strpos(o.{s}, f.{s}) = 1)"
)

SUMMARY_SQL = """
WITH fbref AS ({fbref}),
other AS ({other}),
joined AS (
    SELECT f.league, f.season, f.home, f.away, f.hg, f.ag,
           COUNT(o.home)                                         AS candidates,
           MAX(CASE WHEN f.hg = o.hg AND f.ag = o.ag THEN 1 ELSE 0 END) AS agrees,
           MIN(CAST(o.hg AS varchar) || '-' || CAST(o.ag AS varchar))   AS other_score
    FROM fbref f
    LEFT JOIN other o
      ON  f.league = o.league
      AND f.season = o.season
      AND {home_match}
      AND {away_match}
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT league,
       COUNT(*)                                                        AS fbref,
       SUM(CASE WHEN candidates = 1 THEN 1 ELSE 0 END)                 AS matched,
       SUM(CASE WHEN candidates = 1 AND agrees = 1 THEN 1 ELSE 0 END)  AS agree,
       SUM(CASE WHEN candidates = 1 AND agrees = 0 THEN 1 ELSE 0 END)  AS mismatch,
       SUM(CASE WHEN candidates > 1 THEN 1 ELSE 0 END)                 AS ambiguous
FROM joined
GROUP BY league
ORDER BY league
"""

EXAMPLES_SQL = """
WITH fbref AS ({fbref}),
other AS ({other}),
joined AS (
    SELECT f.league, f.season, f.home, f.away,
           CAST(f.hg AS varchar) || '-' || CAST(f.ag AS varchar) AS fbref_score,
           COUNT(o.home) AS candidates,
           MAX(CASE WHEN f.hg = o.hg AND f.ag = o.ag THEN 1 ELSE 0 END) AS agrees,
           MIN(CAST(o.hg AS varchar) || '-' || CAST(o.ag AS varchar))   AS other_score
    FROM fbref f
    LEFT JOIN other o
      ON  f.league = o.league
      AND f.season = o.season
      AND {home_match}
      AND {away_match}
    GROUP BY 1, 2, 3, 4, 5
)
SELECT league, season, home, away, fbref_score, other_score
FROM joined
WHERE candidates = 1 AND agrees = 0
ORDER BY league, season, home
LIMIT {limit}
"""


def get_conn():
    import trino
    password = os.environ.get('TRINO_PASSWORD', '')
    user = os.environ.get('TRINO_USER', 'airflow')
    kw = dict(
        host=os.environ.get('TRINO_HOST', 'trino'),
        port=int(os.environ.get('TRINO_PORT', 8443)),
        user=user,
        catalog='iceberg',
    )
    if password:
        kw.update(
            http_scheme='https',
            auth=trino.auth.BasicAuthentication(user, password),
            verify=False,
        )
    return trino.dbapi.connect(**kw)


def _render(template: str, source: str, **extra) -> str:
    return template.format(
        fbref=FBREF_SQL,
        other=SOURCES[source],
        home_match=_TEAM_MATCH.format(s='home'),
        away_match=_TEAM_MATCH.format(s='away'),
        **extra,
    )


def validate(c, source: str, examples: int) -> None:
    logger.info(f"--- {source} ---")
    c.execute(_render(SUMMARY_SQL, source))
    rows = c.fetchall()

    print(f"\n{source}")
    print(f"{'league':<20} {'fbref':>7} {'matched':>8} {'agree':>7} "
          f"{'mismatch':>9} {'ambig':>6} {'agree%':>7}")
    tot = [0, 0, 0, 0, 0]
    for league, fbref, matched, agree, mismatch, ambiguous in rows:
        pct = f"{100.0 * agree / matched:.1f}" if matched else '—'
        print(f"{league:<20} {fbref:>7} {matched:>8} {agree:>7} "
              f"{mismatch:>9} {ambiguous:>6} {pct:>7}")
        for i, v in enumerate((fbref, matched, agree, mismatch, ambiguous)):
            tot[i] += v
    pct = f"{100.0 * tot[2] / tot[1]:.1f}" if tot[1] else '—'
    print(f"{'TOTAL':<20} {tot[0]:>7} {tot[1]:>8} {tot[2]:>7} "
          f"{tot[3]:>9} {tot[4]:>6} {pct:>7}")

    if examples and tot[3]:
        c.execute(_render(EXAMPLES_SQL, source, limit=examples))
        print(f"\n  mismatching fixtures (fbref vs {source}):")
        for league, season, home, away, fs, os_ in c.fetchall():
            print(f"    {league:<20} {season}  {home} vs {away}: {fs} vs {os_}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=sorted(SOURCES),
                        help='validate against one source (default: all)')
    parser.add_argument('--examples', type=int, default=10,
                        help='how many mismatching fixtures to print (0 = none)')
    args = parser.parse_args()

    conn = get_conn()
    c = conn.cursor()
    for source in ([args.source] if args.source else sorted(SOURCES)):
        try:
            validate(c, source, args.examples)
        except Exception:
            logger.exception(f"{source}: validation failed")
            sys.exit(1)


if __name__ == '__main__':
    main()
