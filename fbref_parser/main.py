#!/usr/bin/env python3
"""
William Saliba parser с использованием Playwright
Парсит все секции статистики в одну объединенную таблицу
"""
import pandas as pd
import time
import logging
import tempfile
import os
import re
import gzip
from io import BytesIO
from datetime import datetime, timezone
from hdfs import InsecureClient
from playwright.sync_api import sync_playwright

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SalibaPlaywrightParser:
    def __init__(self):
        self.base_url = "https://fbref.com"
        self.saliba_url = "https://fbref.com/en/players/972aeb2a/all_comps/William-Saliba-Stats---All-Competitions"
        self.fbref_id = "972aeb2a"
        self.player_slug = "William-Saliba"
        
        # HDFS настройки
        self.hdfs_host = os.getenv('HDFS_WEB_URL', 'http://namenode:9870')
        self.hdfs_user = os.getenv('HDFS_USER', 'airflow')
        self.hdfs_path = "/fbref/william_saliba"
        
        # Секции для парсинга (используем expanded версии таблиц)
        self.target_sections = [
            'stats_standard_expanded',
            'stats_shooting_expanded', 
            'stats_passing_expanded',
            'stats_pass_types_expanded',
            'stats_gca_expanded',
            'stats_defense_expanded',
            'stats_possession_expanded',
            'stats_playing_time_expanded',
            'stats_misc_expanded'
        ]
        

        
    def clean_competition_name(self, comp_name):
        """Очищает название соревнования от номеров"""
        if not isinstance(comp_name, str):
            return comp_name
            
        # Убираем паттерны типа "1. Premier League" -> "Premier League"
        cleaned = re.sub(r'^\d+\.\s*', '', comp_name.strip())
        return cleaned
        
    def parse_saliba_with_playwright(self):
        """Парсинг William Saliba с Playwright"""
        logger.info("=" * 60)
        logger.info("🎯 WILLIAM SALIBA PLAYWRIGHT PARSER")
        logger.info("=" * 60)
        logger.info(f"🔗 Target URL: {self.saliba_url}")
        logger.info(f"📊 Target sections: {len(self.target_sections)}")

        logger.info("=" * 60)
        
        all_tables_data = []
        player_info = {}
        
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
            
            # Создаем контекст с реальными заголовками
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='en-US',
                timezone_id='America/New_York'
            )
            
            page = context.new_page()
            
            # Добавляем дополнительные заголовки
            page.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            
            # Переходим на страницу с увеличенным таймаутом
            logger.info(f"📋 Loading page: {self.saliba_url}")
            page.goto(self.saliba_url, wait_until='domcontentloaded', timeout=60000)
            
            # Улучшенный обход Cloudflare
            logger.info("⏱️ Waiting for Cloudflare...")
            max_attempts = 3
            
            for attempt in range(max_attempts):
                time.sleep(8 + attempt * 5)  # Увеличиваем задержку с каждой попыткой
                
                page_content = page.content().lower()
                page_title = page.title().lower()
                
                # Проверяем множественные индикаторы Cloudflare
                cloudflare_indicators = [
                    "just a moment", "cloudflare", "checking your browser",
                    "please wait", "ddos protection", "security check"
                ]
                
                if any(indicator in page_title or indicator in page_content for indicator in cloudflare_indicators):
                    logger.warning(f"🛡️ Cloudflare detected (attempt {attempt + 1}/{max_attempts}), waiting...")
                    
                    if attempt < max_attempts - 1:
                        # Имитируем человеческое поведение
                        page.mouse.move(100, 100)
                        time.sleep(2)
                        page.mouse.move(200, 200)
                        time.sleep(15 + attempt * 10)  # Прогрессивно увеличиваем ожидание
                        
                        # Пробуем перезагрузить
                        page.reload(wait_until='domcontentloaded', timeout=90000)
                        time.sleep(5)
                    else:
                        logger.error("❌ Failed to bypass Cloudflare after all attempts")
                else:
                    logger.info("✅ Cloudflare bypass successful")
                    break
            
            # Отладочная информация
            page_title = page.title()
            page_url = page.url
            logger.info(f"📋 Page loaded - Title: {page_title}")
            logger.info(f"📋 Page URL: {page_url}")
            # Сохраняем RAW HTML снапшот
            try:
                html = page.content()
                self.save_raw_html(html, source_url=page_url)
            except Exception as raw_err:
                logger.warning(f"RAW save warning: {raw_err}")
            
            # Извлекаем базовую информацию о игроке
            self.extract_player_info(page, player_info)
            
            # Парсим каждую секцию
            for section_id in self.target_sections:
                logger.info(f"📊 Processing section: {section_id}")
                section_data = self.parse_section(page, section_id)
                
                if section_data is not None and len(section_data) > 0:
                    # Добавляем метку секции
                    section_data['section'] = section_id.replace('stats_', '').replace('_expanded', '')
                    all_tables_data.append(section_data)
                    logger.info(f"✅ Section {section_id}: {len(section_data)} rows extracted")
                else:
                    logger.warning(f"⚠️ Section {section_id}: no data found")
                    
            browser.close()
            
        # Объединяем все таблицы в одну
        if all_tables_data:
            combined_data = self.combine_all_tables(all_tables_data, player_info)
            logger.info(f"🎉 Combined data shape: {combined_data.shape}")
            return combined_data
        else:
            logger.error("❌ No data extracted from any section")
            return None
        
    def extract_player_info(self, page, player_info):
        """Извлекает базовую информацию о игроке"""
        logger.info("👤 Extracting player basic info...")
        
        try:
            # Имя игрока - ищем более специфичный селектор
            name_selectors = ['h1[itemProp="name"]', 'h1', '.player_name', '[data-stat="player"]']
            name_found = False
            
            for selector in name_selectors:
                name_elem = page.query_selector(selector)
                if name_elem:
                    name_text = name_elem.inner_text().strip()
                    if name_text and 'William' in name_text:
                        player_info['name'] = name_text
                        logger.info(f"👤 Name: {player_info['name']} (using selector: {selector})")
                        name_found = True
                        break
                        
            if not name_found:
                # Fallback - пробуем все h1 элементы
                all_h1 = page.query_selector_all('h1')
                logger.info(f"👤 Found {len(all_h1)} h1 elements:")
                for i, h1 in enumerate(all_h1):
                    text = h1.inner_text().strip()
                    logger.info(f"   h1[{i}]: {text}")
                    if 'William' in text or 'Saliba' in text:
                        player_info['name'] = text
                        logger.info(f"👤 Name found: {text}")
                        break
            
            # Мета информация из div#meta
            meta_div = page.query_selector('div#meta')
            if meta_div:
                meta_text = meta_div.inner_text()
                
                # Позиция
                pos_match = re.search(r'Position:\s*([^▪\n]+)', meta_text)
                if pos_match:
                    player_info['position'] = pos_match.group(1).strip()
                    logger.info(f"⚽ Position: {player_info['position']}")
                
                # Дата рождения
                birth_match = re.search(r'Born:\s*([^▪\n]+?)(?:\s+in\s+|$)', meta_text)
                if birth_match:
                    player_info['birth_date'] = birth_match.group(1).strip()
                    logger.info(f"🎂 Birth: {player_info['birth_date']}")
                
                # Рост и вес
                height_match = re.search(r'(\d+)cm', meta_text)
                if height_match:
                    player_info['height'] = f"{height_match.group(1)}cm"
                    logger.info(f"📏 Height: {player_info['height']}")
                    
                weight_match = re.search(r'(\d+)kg', meta_text)
                if weight_match:
                    player_info['weight'] = f"{weight_match.group(1)}kg"
                    logger.info(f"⚖️ Weight: {player_info['weight']}")
                
                # Национальность
                nationality_match = re.search(r'National Team:\s*([A-Za-z\s]+)', meta_text)
                if nationality_match:
                    nationality = nationality_match.group(1).strip()
                    nationality = re.sub(r'[^\w\s]', '', nationality).strip()
                    if nationality:
                        player_info['nationality'] = nationality
                        logger.info(f"🏴 Nationality: {player_info['nationality']}")
        
        except Exception as e:
            logger.error(f"❌ Error extracting player info: {e}")
    
    def parse_section(self, page, section_id):
        """Парсит конкретную секцию таблицы"""
        try:
            # Находим таблицу по ID
            table_selector = f'table#{section_id}'
            table_element = page.query_selector(table_selector)
            
            if not table_element:
                # Отладочная информация - показываем доступные таблицы
                all_tables = page.query_selector_all('table')
                logger.warning(f"⚠️ Table {section_id} not found")
                logger.info(f"📊 Available tables on page: {len(all_tables)}")
                
                for i, table in enumerate(all_tables[:20]):  # Показываем первые 20
                    table_id = table.get_attribute('id') or 'no-id'
                    table_class = table.get_attribute('class') or 'no-class'
                    # Показываем больше деталей для таблиц с ID
                    if table_id != 'no-id':
                        logger.info(f"   ⭐ Table[{i}]: id='{table_id}', class='{table_class}'")
                    else:
                        # Для таблиц без ID смотрим на содержимое заголовков
                        headers = table.query_selector_all('th')
                        if headers:
                            header_texts = [h.inner_text().strip() for h in headers[:3]]
                            logger.info(f"   Table[{i}]: id='{table_id}', headers={header_texts}")
                        else:
                            logger.info(f"   Table[{i}]: id='{table_id}', class='{table_class}'")
                
                return None
                
            # Используем pandas для парсинга
            return self.parse_section_with_pandas(page, section_id)
            
            # Очищаем названия соревнований
            comp_columns = [col for col in df.columns if 'comp' in col.lower() or col == 'Comp']
            for comp_col in comp_columns:
                if comp_col in df.columns:
                    df[comp_col] = df[comp_col].apply(self.clean_competition_name)
                    logger.info(f"✅ Cleaned competition names in {comp_col}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error parsing section {section_id}: {e}")
            return None
    
    def parse_section_with_pandas(self, page, section_id):
        """Парсит секцию через pandas с улучшенной обработкой"""
        try:
            # Получаем HTML всей страницы
            page_html = page.content()
            
            # Ищем таблицу по ID в HTML
            import re
            table_pattern = f'<table[^>]*id="{section_id}"[^>]*>.*?</table>'
            table_match = re.search(table_pattern, page_html, re.DOTALL | re.IGNORECASE)
            
            if not table_match:
                return None
            
            table_html = table_match.group(0)
            
            # Парсим через pandas
            import warnings
            warnings.filterwarnings('ignore', category=FutureWarning)
            
            dfs = pd.read_html(table_html, header=[0, 1])
            if not dfs:
                return None
                
            df = dfs[0]
            
            # Обрабатываем multi-level колонки
            if isinstance(df.columns, pd.MultiIndex):
                # Создаем новые имена колонок
                new_columns = []
                for col in df.columns:
                    if isinstance(col, tuple):
                        # Очищаем от 'Unnamed' и соединяем части
                        parts = [str(part).strip() for part in col if not str(part).startswith('Unnamed')]
                        if parts:
                            new_col = '_'.join(parts)
                        else:
                            # Если все части Unnamed, используем первую
                            new_col = str(col[0])
                    else:
                        new_col = str(col)
                    new_columns.append(new_col)
                
                df.columns = new_columns
            
            # Проверяем и фиксим стандартные колонки
            # Ищем и заменяем все проблемные колонки на правильные названия
            rename_dict = {}
            for col in df.columns:
                # Основные колонки
                if col in ['Unnamed: 0_level_0', 'Unnamed: 0']:
                    rename_dict[col] = 'Season'
                elif col in ['Unnamed: 1_level_0', 'Unnamed: 1']:
                    rename_dict[col] = 'Age'
                elif col in ['Unnamed: 2_level_0', 'Unnamed: 2']:
                    rename_dict[col] = 'Squad'
                elif col in ['Unnamed: 3_level_0', 'Unnamed: 3']:
                    rename_dict[col] = 'Country'
                elif col in ['Unnamed: 4_level_0', 'Unnamed: 4']:
                    rename_dict[col] = 'Comp'
                elif col in ['Unnamed: 5_level_0', 'Unnamed: 5']:
                    rename_dict[col] = 'MP'
                # Очищаем остальные Unnamed
                elif 'Unnamed:' in col:
                    # Пытаемся извлечь имя из второго уровня
                    parts = col.split('_')
                    if len(parts) > 3:
                        # Берем последнюю часть как имя
                        new_name = parts[-1]
                        if new_name and not new_name.startswith('level'):
                            rename_dict[col] = new_name
            
            if rename_dict:
                df.rename(columns=rename_dict, inplace=True)
                logger.info(f"⚭ Renamed {len(rename_dict)} columns")

            
            logger.info(f"📊 {section_id} shape: {df.shape}")
            logger.info(f"📊 First 5 columns: {list(df.columns[:5])}")
            
            # Очищаем названия соревнований
            comp_columns = [col for col in df.columns if 'comp' in col.lower() or col == 'Comp']
            for comp_col in comp_columns:
                if comp_col in df.columns:
                    df[comp_col] = df[comp_col].apply(self.clean_competition_name)
                    logger.info(f"✅ Cleaned competition names in {comp_col}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error parsing section {section_id}: {e}")
            return None
    
    def combine_all_tables(self, all_tables_data, player_info):
        """Объединяет все таблицы в одну"""
        logger.info("=" * 60)
        logger.info("🔗 COMBINING ALL TABLES")
        logger.info("=" * 60)
        
        combined_rows = []
        
        for table_data in all_tables_data:
            section_name = table_data['section'].iloc[0]  # Берем первое значение из Series
            df = table_data.drop(columns=['section'])
            
            logger.info(f"📊 Processing section: {section_name}")
            logger.info(f"   Shape: {df.shape}")
            logger.info(f"   First 5 columns: {list(df.columns[:5])}")
            
            # Добавляем префикс секции к колонкам (кроме общих)
            # Обновленный список общих колонок с более точными названиями
            common_columns = ['Season', 'Age', 'Squad', 'Country', 'Comp', 'LgRank', '90s', 'Matches']
            
            # Переименовываем колонки
            new_columns = {}
            for col in df.columns:
                # Более точная проверка общих колонок
                is_common = False
                col_lower = col.lower()
                
                # Проверяем точные совпадения для общих колонок
                if col in common_columns:
                    is_common = True
                # Проверяем специфичные паттерны
                elif col_lower in ['season', 'age', 'squad', 'country', 'comp', 'lgrank', '90s', 'matches']:
                    is_common = True
                # Проверяем, не является ли это частью составного названия
                elif any(col_lower == common.lower() for common in common_columns):
                    is_common = True
                
                if not is_common and col != 'data_source_section':
                    new_col_name = f"{section_name}_{col}"
                    new_columns[col] = new_col_name
            
            if new_columns:
                df = df.rename(columns=new_columns)
                logger.info(f"📊 Renamed {len(new_columns)} columns with {section_name} prefix")
            
            # Добавляем информацию о секции
            df['data_source_section'] = section_name
            
            combined_rows.append(df)
            logger.info(f"✅ Added {len(df)} rows from {section_name}")
        
        if not combined_rows:
            logger.error("❌ No data to combine")
            return None
            
        # Объединяем все таблицы по общим колонкам
        logger.info("🔗 Merging tables...")
        
        # Начинаем с первой таблицы
        result_df = combined_rows[0].copy()
        
        # Последовательно присоединяем остальные таблицы
        for i, next_df in enumerate(combined_rows[1:], 1):
            # Находим актуальные общие колонки в обеих таблицах
            actual_common_cols = []
            for col in result_df.columns:
                if col in next_df.columns and col in common_columns + ['Season', 'Age', 'Squad', 'Country', 'Comp']:
                    actual_common_cols.append(col)
            
            if actual_common_cols:
                logger.info(f"   Merging table {i+1} on columns: {actual_common_cols}")
                # Убираем дубликаты перед merge
                result_df = result_df.drop_duplicates(subset=actual_common_cols)
                next_df = next_df.drop_duplicates(subset=actual_common_cols)
                
                result_df = pd.merge(
                    result_df, 
                    next_df, 
                    on=actual_common_cols, 
                    how='outer',
                    suffixes=('', f'_dup_{i}')
                )
                
                # Удаляем колонки с суффиксом _dup
                dup_cols = [col for col in result_df.columns if '_dup_' in col]
                if dup_cols:
                    result_df = result_df.drop(columns=dup_cols)
                    logger.info(f"   Removed {len(dup_cols)} duplicate columns")
            else:
                logger.warning(f"   No common columns for table {i+1}, concatenating...")
                result_df = pd.concat([result_df, next_df], ignore_index=True, sort=False)
        
        # Добавляем базовую информацию о игроке
        for key, value in player_info.items():
            result_df[f'player_{key}'] = value
            
        # Добавляем метки
        result_df['team'] = 'arsenal'
        result_df['parsed_at'] = datetime.now().isoformat()
        
        logger.info(f"🎉 Final combined shape: {result_df.shape}")
        logger.info(f"🎉 Final columns count: {len(result_df.columns)}")
        logger.info(f"🎉 First 10 columns: {list(result_df.columns[:10])}")
        
        return result_df

    # ---------- NEW: RAW and NDJSON writers ----------
    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _today_date(self) -> str:
        return datetime.now(timezone.utc).date().isoformat()

    def save_raw_html(self, html: str, source_url: str) -> None:
        """Сохраняет RAW HTML снапшот страницы в HDFS (gzip)."""
        ingest_date = self._today_date()
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        hdfs_client = InsecureClient(self.hdfs_host, user=self.hdfs_user)

        raw_dir = f"/data/raw/fbref/player={self.fbref_id}/ingest_date={ingest_date}"
        raw_path = f"{raw_dir}/all_comps_{ts}.html.gz"

        logger.info(f"💾 Saving RAW HTML to HDFS: {raw_path}")
        # gzip in-memory
        buffer = BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as gz:
            gz.write(html.encode('utf-8'))
        data = buffer.getvalue()

        # Ensure dir and write
        hdfs_client.makedirs(raw_dir, permission=0o755)
        with hdfs_client.write(raw_path, overwrite=True) as writer:
            writer.write(data)
        logger.info("✅ RAW HTML saved")

    def build_profile_record(self, player_info: dict, source_url: str) -> dict:
        """Формирует профиль игрока под fbref_player_profile.schema.json"""
        record = {
            "source": "fbref",
            "fbref_id": self.fbref_id,
            "player_slug": self.player_slug,
            "full_name": player_info.get('name') or "William Saliba",
            "known_as": "William Saliba",
            "birth_date": None,
            "age": None,
            "nationalities": [],
            "height_cm": None,
            "weight_kg": None,
            "foot": None,
            "positions": [],
            "current_club": None,
            "shirt_number": None,
            "source_url": source_url,
            "ingest_ts": self._now_iso(),
        }
        # Map available fields
        if player_info.get('birth_date'):
            # fbref birth text may include words; leave as-is or attempt parse outside scope
            record["birth_date"] = None
        if player_info.get('nationality'):
            record["nationalities"] = [player_info['nationality']]
        if player_info.get('height'):
            m = re.search(r"(\d+)cm", player_info['height'])
            if m:
                record["height_cm"] = int(m.group(1))
        if player_info.get('weight'):
            m = re.search(r"(\d+)kg", player_info['weight'])
            if m:
                record["weight_kg"] = int(m.group(1))
        if player_info.get('position'):
            # split by commas/space+brackets
            pos = player_info['position']
            record["positions"] = [p.strip() for p in re.split(r"[,/]+", pos) if p.strip()]
        return record

    def build_season_records(self, combined_df: pd.DataFrame, source_url: str) -> list:
        """Формирует список сезонных записей (минимально необходимые поля + доступные метрики)."""
        required_cols = ['Season', 'Squad', 'Comp']
        for col in required_cols:
            if col not in combined_df.columns:
                logger.warning(f"Missing column in combined data: {col}")
        # Drop rows without season or comp
        df = combined_df.copy()
        df = df[df['Season'].notna() & df['Comp'].notna()]

        records = []
        for _, row in df.iterrows():
            rec = {
                "source": "fbref",
                "fbref_id": self.fbref_id,
                "season": str(row.get('Season')),
                "squad": row.get('Squad') if pd.notna(row.get('Squad')) else None,
                "league_country": None,
                "comp_name": row.get('Comp') if pd.notna(row.get('Comp')) else None,
                "position": None,
                "age_season": float(row.get('Age')) if pd.notna(row.get('Age')) else None,
                "minutes": None,
                "games_played": None,
                "games_starts": None,
                "minutes_per_90s": float(row.get('90s')) if pd.notna(row.get('90s')) else None,
                "ingest_date": self._today_date(),
                "ingest_ts": self._now_iso(),
                "source_url": source_url,
            }
            # Try to map some known fields if present
            for cand in ["playing_time_Min", "playing_time_Minutes", "playing_time_Minutes_Played", "Min"]:
                if cand in row and pd.notna(row[cand]):
                    try:
                        rec["minutes"] = int(float(row[cand]))
                        break
                    except Exception:
                        pass
            for cand in ["standard_MP", "MP", "Matches"]:
                if cand in row and pd.notna(row[cand]):
                    try:
                        rec["games_played"] = int(float(row[cand]))
                        break
                    except Exception:
                        pass
            for cand in ["playing_time_Starts", "Starts"]:
                if cand in row and pd.notna(row[cand]):
                    try:
                        rec["games_starts"] = int(float(row[cand]))
                        break
                    except Exception:
                        pass

            # Attach a few metrics if available
            extra_metrics = {}
            for metric in [
                "standard_Gls", "standard_Ast", "standard_xG", "standard_xA",
                "defense_Tkl", "defense_Int", "defense_Blocks", "defense_Clearances",
                "passing_Cmp", "passing_Att", "passing_Cmp%"
            ]:
                if metric in row and pd.notna(row[metric]):
                    val = row[metric]
                    # Convert percentages to float
                    if metric.endswith('%'):
                        try:
                            val = float(val)
                        except Exception:
                            pass
                    extra_metrics[metric] = val
            rec.update(extra_metrics)
            records.append(rec)
        return records

    def write_ndjson_hdfs(self, records: list, hdfs_dir: str, filename: str) -> str:
        """Пишет список dict в NDJSON файл в HDFS (создаёт директорию)."""
        import json
        hdfs_client = InsecureClient(self.hdfs_host, user=self.hdfs_user)
        hdfs_client.makedirs(hdfs_dir, permission=0o755)
        hdfs_path = f"{hdfs_dir}/{filename}"
        logger.info(f"💾 Writing NDJSON to {hdfs_path}")
        with hdfs_client.write(hdfs_path, overwrite=True, encoding='utf-8') as writer:
            for rec in records:
                writer.write(json.dumps(rec, ensure_ascii=False) + "\n")
        return hdfs_path
    
    def save_to_hdfs(self, data):
        """Сохранение данных в HDFS"""
        logger.info("=" * 60)
        logger.info("💾 SAVING DATA TO STORAGE")
        logger.info("=" * 60)
        
        logger.info(f"📊 DataFrame Shape: {data.shape}")
        logger.info(f"📊 DataFrame Columns: {len(data.columns)}")
        
        try:
            hdfs_client = InsecureClient(self.hdfs_host, user=self.hdfs_user)
            
            with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as tmp_file:
                data.to_parquet(tmp_file.name, index=False)
                
                hdfs_file_path = f"{self.hdfs_path}/william_saliba_combined.parquet"
                
                logger.info(f"📁 Target HDFS Path: {hdfs_file_path}")
                logger.info(f"🌐 HDFS Host: {self.hdfs_host}")
                
                with open(tmp_file.name, 'rb') as local_file:
                    with hdfs_client.write(hdfs_file_path, overwrite=True) as hdfs_file:
                        hdfs_file.write(local_file.read())
                
                os.unlink(tmp_file.name)
                
            logger.info("✅ Successfully saved to HDFS!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save to HDFS: {e}")
            logger.info("🔄 Falling back to local storage...")
            
            # Fallback на локальное сохранение
            local_path = "william_saliba_combined.parquet"
            data.to_parquet(local_path, index=False)
            
            logger.info(f"💾 Saved locally: {local_path}")
            logger.info(f"📊 File size: {os.path.getsize(local_path)} bytes")
            return False
    
    
    
    def run(self):
        """Основной метод"""
        logger.info("🚀 Starting William Saliba Playwright parser")
        
        start_time = datetime.now()
        
        # Парсим данные
        combined_data = self.parse_saliba_with_playwright()
        if combined_data is None:
            logger.error("❌ Failed to parse William Saliba data")
            return False
        
        # Построить профиль и сезонные записи + сохранить как NDJSON в HDFS
        profile = self.build_profile_record({}, self.saliba_url)
        season_records = self.build_season_records(combined_data, self.saliba_url)
        try:
            base_dir = f"/data/silver/fbref/player_id={self.fbref_id}"
            self.write_ndjson_hdfs([profile], base_dir + "/profile", f"{self.player_slug}_profile.ndjson")
            self.write_ndjson_hdfs(season_records, base_dir + "/player_season_stats", f"{self.player_slug}_season_stats.ndjson")
            logger.info("✅ NDJSON written to HDFS (profile and season stats)")
        except Exception as e:
            logger.warning(f"Failed to write NDJSON to HDFS: {e}")

        # Сохраняем данные
        hdfs_success = self.save_to_hdfs(combined_data)
        
        # DDL генерируется отдельным скриптом generate_trino_ddl.py
        trino_success = False
        
        # Итоговый отчет
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("🎉 WILLIAM SALIBA PLAYWRIGHT PARSER - FINAL REPORT")
        logger.info("=" * 60)
        logger.info(f"⏱️ Total Duration: {duration}")
        logger.info(f"👤 Player: William Saliba")
        logger.info(f"⚽ Team: Arsenal")
        logger.info(f"💾 Data saved to HDFS: {'✅ YES' if hdfs_success else '❌ NO (local fallback)'}")
        logger.info(f"💡 Trino table: Используйте generate_trino_ddl.py для создания таблицы")
        logger.info(f"📊 Total rows: {len(combined_data)}")
        logger.info(f"📊 Total columns: {len(combined_data.columns)}")
        logger.info(f"📊 Sections parsed: {len(self.target_sections)}")
        
        logger.info("🎯 NEXT STEPS:")
        logger.info("   1. Создать DDL: python3 generate_trino_ddl.py")
        logger.info("   2. Загрузить в HDFS: ./upload_to_hdfs.sh")
        logger.info("   3. Создать таблицу: docker exec -i trino trino < auto_create_saliba_table.sql")
        logger.info("   4. Trino Web UI: http://localhost:8081")
        
        # Показываем образец данных
        logger.info("📋 SAMPLE DATA:")
        logger.info("Common columns:")
        common_cols = ['Season', 'Age', 'Squad', 'Country', 'Comp']
        for col in common_cols:
            if col in combined_data.columns:
                unique_vals = combined_data[col].dropna().unique()[:5]
                logger.info(f"   {col}: {list(unique_vals)}")
        
        logger.info("📊 Statistics columns (sample):")
        stat_cols = [col for col in combined_data.columns if any(sec in col for sec in ['standard_', 'shooting_', 'passing_'])][:10]
        for col in stat_cols:
            logger.info(f"   {col}")
        
        logger.info("=" * 60)
        logger.info("✅ PARSING COMPLETED SUCCESSFULLY!")
        logger.info("=" * 60)
        
        return True

def main():
    """Точка входа"""
    parser = SalibaPlaywrightParser()
    success = parser.run()
    
    if success:
        logger.info("✅ William Saliba Playwright parser completed successfully")
        return 0
    else:
        logger.error("❌ William Saliba Playwright parser failed")
        return 1

if __name__ == "__main__":
    exit(main())