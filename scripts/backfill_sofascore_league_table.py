"""One-off: backfill historical bronze.sofascore_league_table (APL 2013-2024).
read_league_table only captures the CURRENT season (tournament landing view),
so history was never written. Here we resolve each season_id from the public
/seasons map and pull /standings/total directly (tls_requests + proxy), reusing
normalize_standing + save_to_iceberg for an identical schema. Idempotent per
(league, season) partition; leaves the existing 2526 partition untouched.
"""
import os, sys, random, logging
import pandas as pd
from curl_cffi.requests import Session

sys.path.insert(0, '/opt/airflow'); sys.path.insert(0, '/opt/airflow/dags')
from scrapers.sofascore import SofaScoreScraper
from scrapers.sofascore.camoufox_capture import normalize_standing, season_short_to_label

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
LEAGUE = 'ENG-Premier League'; UT = 17
YEARS = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
PROXIES = [l.strip() for l in open('/opt/airflow/proxys.txt') if l.strip()]

def sess():
    h, p, u, w = random.choice(PROXIES).split(':'); url = f'http://{u}:{w}@{h}:{p}'
    s = Session(impersonate='chrome120'); s.proxies = {'http': url, 'https': url}; return s

seasons = sess().get(f'https://api.sofascore.com/api/v1/unique-tournament/{UT}/seasons', timeout=30).json()['seasons']
ymap = {x['year']: x['id'] for x in seasons}
saved = 0
with SofaScoreScraper(leagues=[LEAGUE], seasons=[YEARS[0]], proxy_file='/opt/airflow/proxys.txt') as sc:
    for y in YEARS:
        short = f"{str(y)[2:4]}{int(str(y)[2:4]) + 1:02d}"
        label = season_short_to_label(short)
        sid = ymap.get(label)
        if not sid:
            print(f'SKIP {short}: no sid for {label}'); continue
        try:
            r = sess().get(f'https://api.sofascore.com/api/v1/unique-tournament/{UT}/season/{sid}/standings/total', timeout=30)
            rows = r.json().get('standings', [{}])[0].get('rows', [])
        except Exception as e:
            print(f'SKIP {short}: fetch error {e}'); continue
        if not rows:
            print(f'SKIP {short}: 0 rows'); continue
        df = pd.DataFrame([normalize_standing(x) for x in rows])
        for c in ('mp', 'w', 'd', 'l', 'gf', 'ga', 'gd', 'pts'):
            df[c] = df[c].astype('Int64')
        df['league'] = LEAGUE; df['season'] = short
        df = sc._add_metadata(df, 'league_table')
        sc.save_to_iceberg(df=df, table_name='sofascore_league_table',
                           partition_cols=['league', 'season'],
                           replace_partitions=['league', 'season'], min_replace_ratio=None)
        saved += 1
        print(f'SAVED {short}: {len(df)} rows (sid={sid})')
print(f'DONE: {saved}/{len(YEARS)} seasons written')
