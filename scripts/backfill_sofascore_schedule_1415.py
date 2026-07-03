"""One-off: backfill the single missing schedule season APL 2014/15 (sid=8186).
match_capture built the detail tables via discovery, but read_schedule never
wrote 1415. Pull the full season from /events/last pagination directly and
write via normalize_event + save_to_iceberg (same schema as read_schedule).
Idempotent on the (league, season='1415') partition.
"""
import sys, random, logging
import pandas as pd
from curl_cffi.requests import Session

sys.path.insert(0, '/opt/airflow'); sys.path.insert(0, '/opt/airflow/dags')
from scrapers.sofascore import SofaScoreScraper
from scrapers.sofascore.camoufox_capture import normalize_event

logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
LEAGUE = 'ENG-Premier League'; UT = 17; SID = 8186; SEASON = '1415'
PROXIES = [l.strip() for l in open('/opt/airflow/proxys.txt') if l.strip()]

def sess():
    h, p, u, w = random.choice(PROXIES).split(':'); url = f'http://{u}:{w}@{h}:{p}'
    s = Session(impersonate='chrome120'); s.proxies = {'http': url, 'https': url}; return s

events = []; pg = 0
while pg <= 20:
    r = sess().get(f'https://api.sofascore.com/api/v1/unique-tournament/{UT}/season/{SID}/events/last/{pg}', timeout=30)
    if r.status_code != 200:
        print(f'page {pg}: status {r.status_code}'); break
    j = r.json(); events += j.get('events', [])
    if not j.get('hasNextPage'): break
    pg += 1
print(f'fetched {len(events)} events across {pg + 1} pages')

df = pd.DataFrame([normalize_event(e) for e in events])
df['league'] = LEAGUE; df['season'] = SEASON
with SofaScoreScraper(leagues=[LEAGUE], seasons=[2014], proxy_file='/opt/airflow/proxys.txt') as sc:
    df = sc._add_metadata(df, 'schedule')
    sc.save_to_iceberg(df=df, table_name='sofascore_schedule',
                       partition_cols=['league', 'season'],
                       replace_partitions=['league', 'season'], min_replace_ratio=None)
print(f'SAVED schedule {SEASON}: {len(df)} rows')
