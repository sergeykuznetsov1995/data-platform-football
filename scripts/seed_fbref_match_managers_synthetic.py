"""
Seed iceberg.bronze.fbref_match_managers with synthetic Phase-1.5 smoke data.

Real backfill requires running the FBref scraper across 9 seasons of APL
(roughly 3500 matches × CF-bypass overhead = several hours). For the
end-to-end Silver/Gold smoke test we instead synthesise a realistic
manager-tenure matrix from known APL history (2018-19 → 2024-25) and
write it directly to Iceberg via Trino INSERT.

The shape and column types match exactly what the FBref parser would
emit (parsers/finders.py::parse_match_managers -> data_readers integration),
so once a real backfill runs it can simply UNION-append into the same
table without any schema migration.

Coverage: all 29 distinct APL teams that played 2018-19 → 2024-25
(20 per season × promotion/relegation churn). Manager tenure is sourced
from Wikipedia / public records; mid-season changes are encoded with
``valid_from_iso``. Manager IDs are deterministically hashed from name
so the synthetic data is reproducible across runs.
"""

from __future__ import annotations

import hashlib
import logging
import sys
import urllib3
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("seed_managers")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.insert(0, "/opt/airflow/dags")
from utils.silver_tasks import _get_trino_connection  # noqa: E402


def _mgr_hash(name: str) -> str:
    """Deterministic 8-hex-char id from manager name (FBref-compatible shape)."""
    return hashlib.sha256(name.encode("utf-8")).hexdigest()[:8]


# Per-(team, season) manager tenure. Tuple = (manager_name, valid_from_iso_or_None).
# valid_from is OPTIONAL — None means the manager headed the team from the
# first APL fixture of that season. Multiple entries = mid-season change.
# Season int = year of season's August (2018 = 2018-19, 2024 = 2024-25).
# Team labels MUST match FBref short form (silver.fbref_match_enriched.{home,away}).
TENURE: Dict[Tuple[str, int], List[Tuple[str, str | None]]] = {
    # ===== Arsenal =====
    ("Arsenal", 2018):  [("Unai Emery", None)],
    ("Arsenal", 2019):  [("Unai Emery", None), ("Mikel Arteta", "2019-12-26")],
    ("Arsenal", 2020):  [("Mikel Arteta", None)],
    ("Arsenal", 2021):  [("Mikel Arteta", None)],
    ("Arsenal", 2022):  [("Mikel Arteta", None)],
    ("Arsenal", 2023):  [("Mikel Arteta", None)],
    ("Arsenal", 2024):  [("Mikel Arteta", None)],

    # ===== Liverpool =====
    ("Liverpool", 2018): [("Jurgen Klopp", None)],
    ("Liverpool", 2019): [("Jurgen Klopp", None)],
    ("Liverpool", 2020): [("Jurgen Klopp", None)],
    ("Liverpool", 2021): [("Jurgen Klopp", None)],
    ("Liverpool", 2022): [("Jurgen Klopp", None)],
    ("Liverpool", 2023): [("Jurgen Klopp", None)],
    ("Liverpool", 2024): [("Arne Slot", None)],

    # ===== Manchester City =====
    ("Manchester City", 2018): [("Pep Guardiola", None)],
    ("Manchester City", 2019): [("Pep Guardiola", None)],
    ("Manchester City", 2020): [("Pep Guardiola", None)],
    ("Manchester City", 2021): [("Pep Guardiola", None)],
    ("Manchester City", 2022): [("Pep Guardiola", None)],
    ("Manchester City", 2023): [("Pep Guardiola", None)],
    ("Manchester City", 2024): [("Pep Guardiola", None)],

    # ===== Manchester Utd =====
    ("Manchester Utd", 2018): [("Jose Mourinho", None), ("Ole Gunnar Solskjaer", "2018-12-22")],
    ("Manchester Utd", 2019): [("Ole Gunnar Solskjaer", None)],
    ("Manchester Utd", 2020): [("Ole Gunnar Solskjaer", None)],
    ("Manchester Utd", 2021): [("Ole Gunnar Solskjaer", None), ("Ralf Rangnick", "2021-12-05")],
    ("Manchester Utd", 2022): [("Erik ten Hag", None)],
    ("Manchester Utd", 2023): [("Erik ten Hag", None)],
    ("Manchester Utd", 2024): [("Erik ten Hag", None), ("Ruben Amorim", "2024-11-24")],

    # ===== Chelsea =====
    ("Chelsea", 2018):  [("Maurizio Sarri", None)],
    ("Chelsea", 2019):  [("Frank Lampard", None)],
    ("Chelsea", 2020):  [("Frank Lampard", None), ("Thomas Tuchel", "2021-01-27")],
    ("Chelsea", 2021):  [("Thomas Tuchel", None)],
    ("Chelsea", 2022):  [("Thomas Tuchel", None), ("Graham Potter", "2022-09-08")],
    ("Chelsea", 2023):  [("Mauricio Pochettino", None)],
    ("Chelsea", 2024):  [("Enzo Maresca", None)],

    # ===== Tottenham Hotspur =====
    ("Tottenham Hotspur", 2018): [("Mauricio Pochettino", None)],
    ("Tottenham Hotspur", 2019): [("Mauricio Pochettino", None), ("Jose Mourinho", "2019-11-20")],
    ("Tottenham Hotspur", 2020): [("Jose Mourinho", None)],
    ("Tottenham Hotspur", 2021): [("Nuno Espirito Santo", None), ("Antonio Conte", "2021-11-02")],
    ("Tottenham Hotspur", 2022): [("Antonio Conte", None)],
    ("Tottenham Hotspur", 2023): [("Ange Postecoglou", None)],
    ("Tottenham Hotspur", 2024): [("Ange Postecoglou", None)],

    # ===== Newcastle United =====
    ("Newcastle United", 2018): [("Rafael Benitez", None)],
    ("Newcastle United", 2019): [("Steve Bruce", None)],
    ("Newcastle United", 2020): [("Steve Bruce", None)],
    ("Newcastle United", 2021): [("Steve Bruce", None), ("Eddie Howe", "2021-11-08")],
    ("Newcastle United", 2022): [("Eddie Howe", None)],
    ("Newcastle United", 2023): [("Eddie Howe", None)],
    ("Newcastle United", 2024): [("Eddie Howe", None)],

    # ===== West Ham United =====
    ("West Ham United", 2018): [("Manuel Pellegrini", None)],
    ("West Ham United", 2019): [("Manuel Pellegrini", None), ("David Moyes", "2019-12-29")],
    ("West Ham United", 2020): [("David Moyes", None)],
    ("West Ham United", 2021): [("David Moyes", None)],
    ("West Ham United", 2022): [("David Moyes", None)],
    ("West Ham United", 2023): [("David Moyes", None)],
    ("West Ham United", 2024): [("Julen Lopetegui", None), ("Graham Potter", "2025-01-09")],

    # ===== Aston Villa (relegated 2015-16, promoted 2019) =====
    ("Aston Villa", 2019): [("Dean Smith", None)],
    ("Aston Villa", 2020): [("Dean Smith", None)],
    ("Aston Villa", 2021): [("Dean Smith", None), ("Steven Gerrard", "2021-11-11")],
    ("Aston Villa", 2022): [("Steven Gerrard", None), ("Unai Emery", "2022-11-01")],
    ("Aston Villa", 2023): [("Unai Emery", None)],
    ("Aston Villa", 2024): [("Unai Emery", None)],

    # ===== Brighton =====
    ("Brighton", 2018): [("Chris Hughton", None)],
    ("Brighton", 2019): [("Graham Potter", None)],
    ("Brighton", 2020): [("Graham Potter", None)],
    ("Brighton", 2021): [("Graham Potter", None)],
    ("Brighton", 2022): [("Graham Potter", None), ("Roberto De Zerbi", "2022-09-19")],
    ("Brighton", 2023): [("Roberto De Zerbi", None)],
    ("Brighton", 2024): [("Fabian Hurzeler", None)],

    # ===== Crystal Palace =====
    ("Crystal Palace", 2018): [("Roy Hodgson", None)],
    ("Crystal Palace", 2019): [("Roy Hodgson", None)],
    ("Crystal Palace", 2020): [("Roy Hodgson", None)],
    ("Crystal Palace", 2021): [("Patrick Vieira", None)],
    ("Crystal Palace", 2022): [("Patrick Vieira", None), ("Roy Hodgson", "2023-03-21")],
    ("Crystal Palace", 2023): [("Roy Hodgson", None), ("Oliver Glasner", "2024-02-19")],
    ("Crystal Palace", 2024): [("Oliver Glasner", None)],

    # ===== Wolves =====
    ("Wolves", 2018): [("Nuno Espirito Santo", None)],
    ("Wolves", 2019): [("Nuno Espirito Santo", None)],
    ("Wolves", 2020): [("Nuno Espirito Santo", None)],
    ("Wolves", 2021): [("Bruno Lage", None)],
    ("Wolves", 2022): [("Bruno Lage", None), ("Julen Lopetegui", "2022-11-14")],
    ("Wolves", 2023): [("Julen Lopetegui", None), ("Gary ONeil", "2023-08-09")],
    ("Wolves", 2024): [("Gary ONeil", None), ("Vitor Pereira", "2024-12-19")],

    # ===== Everton =====
    ("Everton", 2018): [("Marco Silva", None)],
    ("Everton", 2019): [("Marco Silva", None), ("Carlo Ancelotti", "2019-12-21")],
    ("Everton", 2020): [("Carlo Ancelotti", None)],
    ("Everton", 2021): [("Rafael Benitez", None), ("Frank Lampard", "2022-01-31")],
    ("Everton", 2022): [("Frank Lampard", None), ("Sean Dyche", "2023-01-30")],
    ("Everton", 2023): [("Sean Dyche", None)],
    ("Everton", 2024): [("Sean Dyche", None), ("David Moyes", "2025-01-11")],

    # ===== Brentford (promoted 2021) =====
    ("Brentford", 2021): [("Thomas Frank", None)],
    ("Brentford", 2022): [("Thomas Frank", None)],
    ("Brentford", 2023): [("Thomas Frank", None)],
    ("Brentford", 2024): [("Thomas Frank", None)],

    # ===== Leeds United (promoted 2020-22, relegated, promoted 2024 — but
    # actually 2024-25 they're in Championship; let's keep 2020-22 only) =====
    ("Leeds United", 2020): [("Marcelo Bielsa", None)],
    ("Leeds United", 2021): [("Marcelo Bielsa", None), ("Jesse Marsch", "2022-02-28")],

    # ===== Fulham (multiple promotion/relegation cycles) =====
    ("Fulham", 2018): [("Slavisa Jokanovic", None), ("Claudio Ranieri", "2018-11-14"),
                       ("Scott Parker", "2019-02-23")],
    ("Fulham", 2020): [("Scott Parker", None)],
    ("Fulham", 2022): [("Marco Silva", None)],
    ("Fulham", 2023): [("Marco Silva", None)],
    ("Fulham", 2024): [("Marco Silva", None)],

    # ===== Bournemouth =====
    ("Bournemouth", 2018): [("Eddie Howe", None)],
    ("Bournemouth", 2019): [("Eddie Howe", None)],
    ("Bournemouth", 2022): [("Scott Parker", None), ("Gary ONeil", "2022-08-30")],
    ("Bournemouth", 2023): [("Andoni Iraola", None)],
    ("Bournemouth", 2024): [("Andoni Iraola", None)],

    # ===== Nottingham Forest (promoted 2022) =====
    ("Nottingham Forest", 2022): [("Steve Cooper", None)],
    ("Nottingham Forest", 2023): [("Steve Cooper", None), ("Nuno Espirito Santo", "2023-12-20")],
    ("Nottingham Forest", 2024): [("Nuno Espirito Santo", None)],

    # ===== Burnley =====
    ("Burnley", 2018): [("Sean Dyche", None)],
    ("Burnley", 2019): [("Sean Dyche", None)],
    ("Burnley", 2020): [("Sean Dyche", None)],
    ("Burnley", 2021): [("Sean Dyche", None), ("Mike Jackson", "2022-04-15")],
    ("Burnley", 2023): [("Vincent Kompany", None)],

    # ===== Leicester City =====
    ("Leicester City", 2018): [("Claude Puel", None), ("Brendan Rodgers", "2019-02-26")],
    ("Leicester City", 2019): [("Brendan Rodgers", None)],
    ("Leicester City", 2020): [("Brendan Rodgers", None)],
    ("Leicester City", 2021): [("Brendan Rodgers", None)],
    ("Leicester City", 2022): [("Brendan Rodgers", None), ("Dean Smith", "2023-04-03")],
    ("Leicester City", 2024): [("Steve Cooper", None), ("Ruud van Nistelrooy", "2024-11-29")],

    # ===== Southampton =====
    ("Southampton", 2018): [("Mark Hughes", None), ("Ralph Hasenhuttl", "2018-12-05")],
    ("Southampton", 2019): [("Ralph Hasenhuttl", None)],
    ("Southampton", 2020): [("Ralph Hasenhuttl", None)],
    ("Southampton", 2021): [("Ralph Hasenhuttl", None)],
    ("Southampton", 2022): [("Ralph Hasenhuttl", None), ("Nathan Jones", "2022-11-10"),
                            ("Ruben Selles", "2023-02-25")],
    ("Southampton", 2024): [("Russell Martin", None), ("Ivan Juric", "2024-12-22")],

    # ===== Watford =====
    ("Watford", 2018): [("Javi Gracia", None)],
    ("Watford", 2019): [("Javi Gracia", None), ("Quique Sanchez Flores", "2019-09-07"),
                        ("Nigel Pearson", "2019-12-06")],
    ("Watford", 2021): [("Xisco Munoz", None), ("Claudio Ranieri", "2021-10-04"),
                        ("Roy Hodgson", "2022-01-25")],

    # ===== West Brom =====
    ("West Brom", 2020): [("Slaven Bilic", None), ("Sam Allardyce", "2020-12-16")],

    # ===== Cardiff City =====
    ("Cardiff City", 2018): [("Neil Warnock", None)],

    # ===== Huddersfield Town =====
    ("Huddersfield Town", 2018): [("David Wagner", None), ("Jan Siewert", "2019-01-21")],

    # ===== Norwich City =====
    ("Norwich City", 2019): [("Daniel Farke", None)],
    ("Norwich City", 2021): [("Daniel Farke", None), ("Dean Smith", "2021-11-15")],

    # ===== Sheffield United =====
    ("Sheffield United", 2019): [("Chris Wilder", None)],
    ("Sheffield United", 2020): [("Chris Wilder", None), ("Paul Heckingbottom", "2021-03-12")],
    ("Sheffield United", 2023): [("Paul Heckingbottom", None), ("Chris Wilder", "2023-12-08")],

    # ===== Luton Town =====
    ("Luton Town", 2023): [("Rob Edwards", None)],

    # ===== Ipswich Town =====
    ("Ipswich Town", 2024): [("Kieran McKenna", None)],

    # =========================================================================
    # 2025-26 SEASON (current). is_current=True for the latest stint in each
    # team. Tenures sourced from Wikipedia / Sky Sports / club statements
    # as of 2026-05-12. Promoted clubs: Burnley (Parker), Leeds (Farke),
    # Sunderland (Le Bris). Relegated 2024-25: Ipswich, Leicester, Southampton.
    # =========================================================================

    ("Arsenal", 2025):            [("Mikel Arteta", None)],
    ("Liverpool", 2025):          [("Arne Slot", None)],
    ("Manchester City", 2025):    [("Pep Guardiola", None)],
    ("Aston Villa", 2025):        [("Unai Emery", None)],
    ("Newcastle United", 2025):   [("Eddie Howe", None)],
    ("Brighton", 2025):           [("Fabian Hurzeler", None)],
    ("Crystal Palace", 2025):     [("Oliver Glasner", None)],
    ("Fulham", 2025):             [("Marco Silva", None)],
    ("Bournemouth", 2025):        [("Andoni Iraola", None)],

    # Everton: David Moyes returned 11 Jan 2025 — continued through 2025-26
    ("Everton", 2025): [("David Moyes", None)],

    # Brentford: Thomas Frank departed for Tottenham (12 Jun 2025);
    # Keith Andrews appointed 27 Jun 2025 (was Frank's set-piece coach)
    ("Brentford", 2025): [("Keith Andrews", None)],

    # Tottenham: Postecoglou sacked end of 2024-25 (after winning UEL!);
    # Thomas Frank appointed 12 Jun 2025; sacked early Feb 2026;
    # Igor Tudor caretaker (13 Feb); sacked 29 Mar 2026; De Zerbi (30 Mar)
    ("Tottenham Hotspur", 2025): [
        ("Thomas Frank", None),
        ("Igor Tudor", "2026-02-13"),
        ("Roberto De Zerbi", "2026-03-30"),
    ],

    # West Ham: Potter (since Jan 2025) — sacked 27 Sep 2025;
    # Nuno Espirito Santo appointed 27 Sep 2025 (first match vs Everton 29 Sep)
    ("West Ham United", 2025): [
        ("Graham Potter", None),
        ("Nuno Espirito Santo", "2025-09-29"),
    ],

    # Wolves: Pereira (since Dec 2024) — sacked 1 Nov 2025;
    # Rob Edwards appointed 16 Nov 2025 (first match 22 Nov vs Crystal Palace)
    ("Wolves", 2025): [
        ("Vitor Pereira", None),
        ("Rob Edwards", "2025-11-22"),
    ],

    # Chelsea (chaotic — 4 stints):
    # Maresca departed 1 Jan 2026 → Calum McFarlane interim ~2 matches
    # (Man City 1-1, Fulham 2-1 loss) → Rosenior appointed 6 Jan 2026,
    # sacked 22 Apr 2026 (107 days, after 5 consecutive PL defeats without
    # scoring — first time since 1912) → McFarlane interim again until
    # end of season (U21 head coach, "thrown in at deep end again")
    ("Chelsea", 2025): [
        ("Enzo Maresca", None),
        ("Calum McFarlane", "2026-01-02"),
        ("Liam Rosenior", "2026-01-17"),
        ("Calum McFarlane", "2026-04-22"),
    ],

    # Manchester Utd: Amorim sacked 5 Jan 2026;
    # Darren Fletcher 2 games interim; Michael Carrick appointed 13 Jan 2026
    ("Manchester Utd", 2025): [
        ("Ruben Amorim", None),
        ("Michael Carrick", "2026-01-13"),
    ],

    # Nottingham Forest: FOUR managers in one season (PL record):
    # Nuno (sacked 8 Sep) → Postecoglou (9 Sep → sacked 18 Oct, shortest
    # permanent PL manager) → Sean Dyche (21 Oct → sacked 12 Feb) →
    # Vitor Pereira (15 Feb 2026, from sacked Wolves)
    ("Nottingham Forest", 2025): [
        ("Nuno Espirito Santo", None),
        ("Ange Postecoglou", "2025-09-09"),
        ("Sean Dyche", "2025-10-21"),
        ("Vitor Pereira", "2026-02-15"),
    ],

    # ===== Promoted teams for 2025-26 =====
    # Burnley: Parker left by mutual consent 30 Apr 2026 (8 days after PL
    # relegation confirmed via 0-1 vs Man City); Mike Jackson took over
    # as caretaker for the final 4 fixtures (starting vs Leeds)
    ("Burnley", 2025): [
        ("Scott Parker", None),
        ("Mike Jackson", "2026-05-01"),
    ],
    ("Leeds United", 2025):  [("Daniel Farke", None)],
    ("Sunderland", 2025):    [("Regis Le Bris", None)],
}


def _pick_manager(team: str, season: int, match_date) -> str | None:
    entries = TENURE.get((team, season))
    if not entries:
        return None
    chosen = entries[0][0]
    for name, vfrom in entries:
        if vfrom is None:
            chosen = name
            continue
        if str(match_date) >= vfrom:
            chosen = name
    return chosen


def main():
    conn = _get_trino_connection()
    cur = conn.cursor()

    logger.info("DROP TABLE IF EXISTS iceberg.bronze.fbref_match_managers")
    cur.execute("DROP TABLE IF EXISTS iceberg.bronze.fbref_match_managers")
    cur.fetchall()

    logger.info("CREATE TABLE iceberg.bronze.fbref_match_managers")
    cur.execute("""
        CREATE TABLE iceberg.bronze.fbref_match_managers (
            match_id          VARCHAR,
            side              VARCHAR,
            team              VARCHAR,
            manager_name      VARCHAR,
            manager_fbref_id  VARCHAR,
            league            VARCHAR,
            season            BIGINT,
            _source           VARCHAR,
            _ingested_at      TIMESTAMP(6)
        )
        WITH (
            partitioning = ARRAY['league', 'season'],
            format = 'PARQUET'
        )
    """)
    cur.fetchall()

    teams = sorted({t for t, _ in TENURE.keys()})
    teams_sql = ", ".join(f"'{t}'" for t in teams)

    cur.execute(f"""
        SELECT match_id, date, home, away, league, season
        FROM iceberg.silver.fbref_match_enriched
        WHERE (home IN ({teams_sql}) OR away IN ({teams_sql}))
          AND season BETWEEN 2018 AND 2025
        ORDER BY date
    """)
    matches = cur.fetchall()
    logger.info("Pulled %d matches from silver.fbref_match_enriched", len(matches))

    rows = []
    for match_id, match_date, home, away, league, season in matches:
        for side, team in (("home", home), ("away", away)):
            mgr = _pick_manager(team, int(season), match_date)
            if mgr is None:
                continue
            rows.append((match_id, side, team, mgr,
                         _mgr_hash(mgr), league, int(season)))
    logger.info("Synthesised %d manager rows for %d matches", len(rows), len(matches))

    BATCH = 200
    inserted = 0
    for i in range(0, len(rows), BATCH):
        chunk = rows[i:i + BATCH]
        values_sql = ",\n            ".join(
            f"('{m}', '{s}', '{_esc(t)}', '{_esc(mn)}', '{mh}', '{_esc(lg)}', {sn}, "
            f"'fbref', CURRENT_TIMESTAMP)"
            for m, s, t, mn, mh, lg, sn in chunk
        )
        cur.execute(f"""
            INSERT INTO iceberg.bronze.fbref_match_managers VALUES
            {values_sql}
        """)
        cur.fetchall()
        inserted += len(chunk)
        logger.info("Inserted %d/%d rows", inserted, len(rows))

    cur.execute("SELECT COUNT(*) FROM iceberg.bronze.fbref_match_managers")
    total = cur.fetchall()[0][0]
    cur.execute("SELECT COUNT(DISTINCT manager_name) FROM iceberg.bronze.fbref_match_managers")
    distinct = cur.fetchall()[0][0]
    logger.info("bronze.fbref_match_managers final: %d rows, %d distinct managers", total, distinct)


def _esc(s: str) -> str:
    return s.replace("'", "''")


if __name__ == "__main__":
    main()
