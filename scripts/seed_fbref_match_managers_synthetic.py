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
"""

from __future__ import annotations

import logging
import os
import sys
import urllib3
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("seed_managers")

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.insert(0, "/opt/airflow/dags")
from utils.silver_tasks import _get_trino_connection  # noqa: E402

# ---------------------------------------------------------------------------
# Manager tenure model — APL known history. Each (team, season) → manager.
# When a mid-season change happened we encode "from <date>" as a second tuple.
# Format: (manager_name, manager_fbref_id, valid_from_date_iso)
# valid_from_date_iso = date of the FIRST match the manager officially headed
# in the given season. Multiple entries for same (team, season) means the
# manager changed mid-season — Bronze parser would emit two distinct names.
# ---------------------------------------------------------------------------

# Synthetic manager_fbref_ids — eight hex chars, deterministic mapping name→hash
_MGR_HASH = {
    "Mikel Arteta":          "12345671",
    "Unai Emery":             "12345672",
    "Jurgen Klopp":           "12345673",
    "Arne Slot":              "12345674",
    "Pep Guardiola":          "12345675",
    "Jose Mourinho":          "12345676",
    "Ole Gunnar Solskjaer":   "12345677",
    "Erik ten Hag":           "12345678",
    "Ruben Amorim":           "12345679",
    "Mauricio Pochettino":    "1234567a",
    "Frank Lampard":          "1234567b",
    "Thomas Tuchel":          "1234567c",
    "Maurizio Sarri":         "1234567d",
    "Enzo Maresca":           "1234567e",
    "Graham Potter":          "1234567f",
    "Mauricio Sarri 2":       "12345680",
}

# Per-team, per-season manager. Tuple (manager_name, valid_from_iso_date_in_season).
# valid_from is OPTIONAL — if None, manager headed the team for all matches
# in that season starting from the season's first APL fixture.
TENURE: Dict[Tuple[str, int], List[Tuple[str, str | None]]] = {
    ("Arsenal", 2018):  [("Unai Emery", None)],
    ("Arsenal", 2019):  [("Unai Emery", None), ("Mikel Arteta", "2019-12-26")],
    ("Arsenal", 2020):  [("Mikel Arteta", None)],
    ("Arsenal", 2021):  [("Mikel Arteta", None)],
    ("Arsenal", 2022):  [("Mikel Arteta", None)],
    ("Arsenal", 2023):  [("Mikel Arteta", None)],
    ("Arsenal", 2024):  [("Mikel Arteta", None)],

    ("Liverpool", 2018): [("Jurgen Klopp", None)],
    ("Liverpool", 2019): [("Jurgen Klopp", None)],
    ("Liverpool", 2020): [("Jurgen Klopp", None)],
    ("Liverpool", 2021): [("Jurgen Klopp", None)],
    ("Liverpool", 2022): [("Jurgen Klopp", None)],
    ("Liverpool", 2023): [("Jurgen Klopp", None)],
    ("Liverpool", 2024): [("Arne Slot", None)],

    ("Manchester City", 2018): [("Pep Guardiola", None)],
    ("Manchester City", 2019): [("Pep Guardiola", None)],
    ("Manchester City", 2020): [("Pep Guardiola", None)],
    ("Manchester City", 2021): [("Pep Guardiola", None)],
    ("Manchester City", 2022): [("Pep Guardiola", None)],
    ("Manchester City", 2023): [("Pep Guardiola", None)],
    ("Manchester City", 2024): [("Pep Guardiola", None)],

    ("Manchester Utd", 2018): [("Jose Mourinho", None), ("Ole Gunnar Solskjaer", "2018-12-22")],
    ("Manchester Utd", 2019): [("Ole Gunnar Solskjaer", None)],
    ("Manchester Utd", 2020): [("Ole Gunnar Solskjaer", None)],
    ("Manchester Utd", 2021): [("Ole Gunnar Solskjaer", None)],
    ("Manchester Utd", 2022): [("Erik ten Hag", None)],
    ("Manchester Utd", 2023): [("Erik ten Hag", None)],
    ("Manchester Utd", 2024): [("Erik ten Hag", None), ("Ruben Amorim", "2024-11-24")],

    ("Chelsea", 2018):  [("Maurizio Sarri", None)],
    ("Chelsea", 2019):  [("Frank Lampard", None)],
    ("Chelsea", 2020):  [("Frank Lampard", None), ("Thomas Tuchel", "2021-01-27")],
    ("Chelsea", 2021):  [("Thomas Tuchel", None)],
    ("Chelsea", 2022):  [("Thomas Tuchel", None), ("Graham Potter", "2022-09-08")],
    ("Chelsea", 2023):  [("Mauricio Pochettino", None)],
    ("Chelsea", 2024):  [("Enzo Maresca", None)],
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

    # Pull match_ids + dates + home/away from the existing Silver enriched view.
    # Restrict to teams present in our TENURE matrix to keep the smoke set
    # focused on managers we can reason about.
    teams = sorted({t for t, _ in TENURE.keys()})
    teams_sql = ", ".join(f"'{t}'" for t in teams)

    cur.execute(f"""
        SELECT match_id, date, home, away, league, season
        FROM iceberg.silver.fbref_match_enriched
        WHERE (home IN ({teams_sql}) OR away IN ({teams_sql}))
          AND season BETWEEN 2018 AND 2024
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
            mgr_id = _MGR_HASH.get(mgr, "00000000")
            rows.append((match_id, side, team, mgr,
                         mgr_id, league, int(season)))
    logger.info("Synthesised %d manager rows for %d matches", len(rows), len(matches))

    # Trino doesn't support multi-VALUES INSERT efficiently for big batches;
    # split into chunks of 200.
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
    logger.info("bronze.fbref_match_managers final count: %d", total)


def _esc(s: str) -> str:
    return s.replace("'", "''")


if __name__ == "__main__":
    main()
