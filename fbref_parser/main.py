import requests
import pandas as pd
from bs4 import BeautifulSoup
from snakebite.client import Client
import pyarrow as pa
import pyarrow.parquet as pq
import os
import time
import random
from io import StringIO
from hdfs import InsecureClient
import re

# HDFS config
HDFS_HOST = "84.201.177.15"
HDFS_PORT = 9000
HDFS_DIR = "/fbref/arsenal_2024_2025"
hdfs = Client(HDFS_HOST, HDFS_PORT, use_trash=False)
HDFS_WEB_URL = "http://84.201.177.15:9870"
hdfs_client = InsecureClient(HDFS_WEB_URL, user='airflow')

BASE_URL = "https://fbref.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
}
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0"
]

SQUAD_URL = "/en/squads/18bb7c10/2024-2025/Arsenal-Stats"
SEASON = "2024-2025"
TEAM = "arsenal"


def safe_get(url, headers, max_retries=5, min_sleep=5, max_sleep=10):
    for i in range(max_retries):
        headers = dict(headers)
        headers["User-Agent"] = random.choice(USER_AGENTS)
        resp = requests.get(url, headers=headers)
        if resp.status_code == 429:
            print(f"[WARN] 429 Too Many Requests for {url}, sleeping 60s...")
            time.sleep(60)
            continue
        resp.raise_for_status()
        sleep_time = random.uniform(min_sleep, max_sleep)
        print(f"[SLEEP] {sleep_time:.1f} seconds before next request...")
        time.sleep(sleep_time)
        return resp
    raise RuntimeError(f"Too many 429 errors for {url}, aborting.")


def save_to_hdfs(df: pd.DataFrame, path: str):
    print(f"[INFO] Сохраняю в HDFS: {path}")
    local_file = "/tmp/temp.parquet"
    # Сбросить multiindex, если есть
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(map(str, col)).strip() for col in df.columns.values]
    df = df.astype(str)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, local_file)
    hdfs_path = path
    hdfs_client.makedirs(os.path.dirname(hdfs_path))
    with open(local_file, 'rb') as reader:
        hdfs_client.write(hdfs_path, reader, overwrite=True)
    print(f"[INFO] Файл успешно загружен в HDFS: {hdfs_path}")


def parse_arsenal_team_stats():
    print(f"[INFO] Получаю HTML команды Арсенал за {SEASON}: {SQUAD_URL}")
    resp = safe_get(BASE_URL + SQUAD_URL, headers=HEADERS)
    soup = BeautifulSoup(resp.text, "lxml")
    # Парсим первую таблицу stats_table (основная статистика команды)
    tables = soup.select('table.stats_table')
    if tables:
        main_table = tables[0]
        df = pd.read_html(StringIO(str(main_table)))[0]
        save_to_hdfs(df, f"{HDFS_DIR}/team_stats.parquet")
    else:
        print("[ERROR] Не найдена таблица статистики команды!")

    # Парсим статистику лиг (турниров), где играл Арсенал (например, summary по турнирам)
    league_tables = []
    for table in soup.select('table.stats_table'):
        caption = table.find('caption')
        if caption and ("Premier League" in caption.text or "Champions League" in caption.text or "Cup" in caption.text):
            league_tables.append(table)
    for table in league_tables:
        df = pd.read_html(StringIO(str(table)))[0]
        name = table.find('caption').text.strip().replace(' ', '_').lower()
        name = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
        save_to_hdfs(df, f"{HDFS_DIR}/league_{name}.parquet")

    # Парсим игроков Арсенала (только из первой таблицы)
    player_ids = set()
    player_urls = set()
    if tables:
        main_table = tables[0]
        for a in main_table.select('a[href*="/en/players/"]'):
            m = re.match(r'/en/players/([^/]+)/', a['href'])
            if m and m.group(1) not in player_ids:
                player_ids.add(m.group(1))
                player_urls.add(f'/en/players/{m.group(1)}/')
        print(f"[OK] Найдено игроков Арсенала: {len(player_urls)}")
    else:
        print("[ERROR] Не найдена ни одна таблица stats_table!")
        return []
    return list(player_urls)


def parse_player_profile(player_url):
    print(f"[INFO] Парсю профиль игрока: {player_url}")
    resp = safe_get(BASE_URL + player_url, headers=HEADERS)
    soup = BeautifulSoup(resp.text, 'lxml')
    profile = {}
    h1 = soup.find('h1', attrs={'itemprop': 'name'}) or soup.find('h1')
    profile['name'] = h1.text.strip() if h1 else None
    for p in soup.select('div#meta p'):
        txt = p.text.strip()
        if 'Born:' in txt:
            profile['birth'] = txt.replace('Born:', '').strip()
        if 'Height:' in txt:
            profile['height'] = txt.replace('Height:', '').strip()
        if 'Position:' in txt:
            profile['position'] = txt.replace('Position:', '').strip()
        if 'Footed:' in txt:
            profile['footed'] = txt.replace('Footed:', '').strip()
        if 'Weight:' in txt:
            profile['weight'] = txt.replace('Weight:', '').strip()
    # Парсим все таблицы статистики игрока (summary, passing, defense и т.д.)
    for table in soup.select('table.stats_table'):
        table_id = table.get('id', 'unknown')
        df = pd.read_html(StringIO(str(table)))[0]
        if not df.empty:
            for col in df.columns:
                val = df.iloc[0][col]
                key = f"{table_id}_{col}".replace(' ', '_').lower()
                profile[key] = val
    print(f"[OK] Спарсен профиль игрока: {profile.get('name', player_url)}")
    return profile


def main():
    # 1. Парсим командную статистику и турниры
    player_urls = parse_arsenal_team_stats()
    # 2. Парсим профили игроков
    players = []
    for i, player_url in enumerate(player_urls):
        try:
            player_data = parse_player_profile(player_url)
            player_data['team'] = TEAM
            player_data['season'] = SEASON
            players.append(player_data)
        except Exception as e:
            print(f"[ERROR] {player_url}: {e}")
    if players:
        df = pd.DataFrame(players)
        save_to_hdfs(df, f"{HDFS_DIR}/players_detailed.parquet")
    else:
        print("[WARN] Нет данных по игрокам для сохранения!")

if __name__ == "__main__":
    main()
