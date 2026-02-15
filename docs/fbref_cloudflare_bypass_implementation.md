# Реализация обхода Cloudflare Turnstile на FBref

**Дата:** 2026-02-07
**Статус:** Реализовано

---

## Проблема

FBref.com возвращает **403 Forbidden** при попытке парсинга данных. Причина — Cloudflare Turnstile CAPTCHA, которая требует выполнения JavaScript для решения интерактивного checkbox.

```
HTTPError: 403 Client Error: Forbidden for url: https://fbref.com/en/comps/
```

### Почему предыдущие решения не работали

| Метод | Проблема |
|-------|----------|
| `soccerdata` + `curl_cffi` | curl_cffi не выполняет JavaScript |
| FlareSolverr | Устарел, не поддерживает Turnstile |
| Tor proxy | Заблокирован FBref |
| CF Cookie Injection | Cookies истекают за 30 минут |

---

## Решение

**nodriver + cf-verify plugin** — полноценный браузер с автоматическим кликом по Turnstile checkbox.

### Компоненты решения

1. **NodriverFBrefScraper** — новый скрапер на базе nodriver
2. **cf-verify plugin** — автоматически кликает по Turnstile checkbox
3. **Residential proxies** — 999 прокси для IP ротации
4. **Xvfb** — виртуальный дисплей для обхода headless detection
5. **Последовательное выполнение** — предотвращение OOM

---

## Изменённые файлы

### Новые файлы

| Файл | Описание |
|------|----------|
| `scrapers/nodriver_fbref_scraper.py` | Основной скрапер с nodriver |
| `tests/unit/scrapers/test_nodriver_fbref_scraper.py` | Unit тесты (24 теста) |
| `tests/integration/scrapers/test_nodriver_fbref_integration.py` | Integration тесты |

### Изменённые файлы

| Файл | Изменения |
|------|-----------|
| `scrapers/utils/proxy_manager.py` | Добавлены методы `get_nodriver_proxy_string()`, `get_nodriver_proxy_dict()`, `get_current_proxy()` |
| `dags/scripts/run_fbref_scraper.py` | Добавлен тип `nodriver`, новые параметры `--cloudflare-wait`, `--cf-verify-retries` |
| `dags/dag_ingest_fbref.py` | `DEFAULT_SCRAPER_TYPE = 'nodriver'`, последовательное выполнение задач |

---

## Архитектура NodriverFBrefScraper

```
NodriverFBrefScraper
├── _proxy_manager: ProxyManager
│   └── get_nodriver_proxy_string() → host:port:user:pass
├── _browser: NodriverBypass
│   ├── use_cf_verify=True
│   ├── cf_verify_max_retries=12
│   └── cloudflare_wait=90.0
├── _fetch_page(url)
│   ├── Rate limiting (5s между запросами)
│   ├── Cloudflare detection
│   └── Proxy rotation on failure
├── read_schedule()
├── read_player_season_stats()
├── read_team_season_stats()
├── read_keeper_stats()
└── scrape_single_stat_type() → memory-efficient
```

---

## Конфигурация DAG

```python
# dags/dag_ingest_fbref.py

DEFAULT_SCRAPER_TYPE = 'nodriver'  # Было 'soccerdata'

# Nodriver settings
USE_NODRIVER = True
NODRIVER_CLOUDFLARE_WAIT = 90.0
NODRIVER_MAX_RETRIES = 5
NODRIVER_CF_VERIFY_RETRIES = 12

# OOM prevention
DAG_CONCURRENCY = 1  # Одна задача одновременно
# Задачи внутри TaskGroups выполняются последовательно
```

---

## Использование

### Ручной запуск скрапера

```bash
# Скрапинг одного типа статистики
python dags/scripts/run_fbref_scraper.py \
    --scraper-type nodriver \
    --proxy-file /opt/airflow/proxys.txt \
    --mode single_stat \
    --stat-type stats \
    --data-category player \
    --leagues "ENG-Premier League" \
    --season 2024 \
    --output /tmp/fbref_player_stats.json

# Скрапинг расписания
python dags/scripts/run_fbref_scraper.py \
    --scraper-type nodriver \
    --proxy-file /opt/airflow/proxys.txt \
    --mode match_data \
    --match-data-type schedule \
    --leagues "ENG-Premier League" \
    --season 2024
```

### Параметры nodriver scraper

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `--cloudflare-wait` | 90.0 | Время ожидания Cloudflare challenge (секунды) |
| `--cf-verify-retries` | 12 | Максимум попыток cf-verify plugin |
| `--max-retries` | 5 | Максимум попыток загрузки страницы |
| `--proxy-file` | — | Путь к файлу с прокси |
| `--headless` | True | Headless режим браузера |
| `--use-xvfb` | True | Использовать Xvfb |

### Запуск DAG

```bash
# В контейнере Airflow
airflow dags test dag_ingest_fbref 2026-02-07

# Проверка статуса
airflow dags state dag_ingest_fbref 2026-02-07
```

---

## Тестирование

### Unit тесты

```bash
# Все unit тесты (24 теста)
pytest tests/unit/scrapers/test_nodriver_fbref_scraper.py -v

# Результат: 24 passed
```

### Integration тесты

```bash
# Реальные запросы к FBref (требует прокси)
pytest tests/integration/scrapers/test_nodriver_fbref_integration.py -v -m integration
```

### Ручная проверка Cloudflare bypass

```python
from scrapers.nodriver_fbref_scraper import NodriverFBrefScraper

with NodriverFBrefScraper(
    leagues=['ENG-Premier League'],
    seasons=[2024],
    proxy_file='proxys.txt',
) as scraper:
    html = scraper._fetch_page("https://fbref.com/en/comps/")

    if html and not scraper._is_cloudflare_blocked(html):
        print("Cloudflare bypass успешен!")
        print(f"HTML length: {len(html)}")
    else:
        print("Cloudflare всё ещё блокирует")

    print(f"Stats: {scraper.get_stats()}")
```

---

## Fallback стратегия

При неудаче nodriver + cf-verify:

1. **Ротация прокси** — до 5 попыток с разными прокси
2. **Увеличение cf_verify_max_retries** — до 15-20
3. **Увеличение cloudflare_wait** — до 120+ секунд
4. **Skip и продолжение** — пропустить stat_type и продолжить

**Примечание:** soccerdata полностью удалён как primary, без fallback на curl_cffi.

---

## Критерии успеха

- [x] Unit тесты проходят (24/24)
- [ ] Cloudflare Turnstile успешно пройден (нет 403 ошибок)
- [ ] DAG `dag_ingest_fbref` выполняется полностью
- [ ] Таблицы сохранены в HDFS/Iceberg
- [ ] Нет OOM ошибок (последовательное выполнение)

---

## Зависимости

```
nodriver>=0.32
nodriver-cf-verify>=0.1.0
opencv-python-headless>=4.8.0
pyvirtualdisplay>=3.0
```

Все зависимости уже присутствуют в `docker/images/airflow/requirements-scraping.txt`.

---

## Ссылки

- [nodriver Documentation](https://github.com/ultrafunkamsterdam/nodriver)
- [nodriver-cf-verify](https://github.com/KlozetLabs/nodriver-cf-bypass)
- [ZenRows - Bypass Cloudflare Python](https://www.zenrows.com/blog/bypass-cloudflare-python)

---

## Changelog

### 2026-02-07

- Создан `NodriverFBrefScraper` с cf-verify интеграцией
- Расширен `ProxyManager` для nodriver формата
- Обновлён DAG на `nodriver` как primary scraper
- Добавлено последовательное выполнение для предотвращения OOM
- Написаны unit и integration тесты
