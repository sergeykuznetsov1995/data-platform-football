# Issue #67 — Evaluate migration of FBref scraper to soccerdata 1.9.0

> Decision: **HOLD** — не мигрируем. Soccerdata 1.9.0 `BaseSeleniumReader` не пробивает Cloudflare в нашем окружении out-of-the-box (множественные blockers без proxy + ломается с residential proxy). Custom-bypass-затраты сводят выгоду «-9800 LOC» к нулю.

Дата: 2026-05-26
Окружение: `airflow-webserver` контейнер (Debian 11, Chromium 120, Xvfb), `/tmp/sd19/soccerdata 1.9.0`, `seleniumbase 4.49.2`

---

## 1. Что проверяли

`soccerdata 1.9.0` (release 2026-04) мигрировала FBref-backend на `BaseSeleniumReader` (seleniumbase + UC mode). Гипотеза issue #67 — если 1.9.0 даёт ≥ той же скорости и coverage что наш `scrapers/fbref/` (~5680 LOC) + `scrapers/base/browser/` (~4146 LOC), миграция снимает огромный maintenance burden (issues #52/#57/#65).

Acceptance criteria #67 ограничивает scope шагом 1 (бенч/coverage/decision), без самой миграции.

## 2. Phase 1 — установка + sanity

- `pip install --target /tmp/sd19 soccerdata==1.9.0 seleniumbase` → OK
- `sd.FBref.__mro__ == [FBref, BaseSeleniumReader, BaseReader, ABC, object]` — подтверждает selenium-backend
- 9 публичных `read_*` методов

## 3. Phase 2 — Coverage audit

| Наша Bronze-таблица | soccerdata 1.9.0 method | Coverage | Gap |
|---|---|---|---|
| `fbref_schedule` | `read_schedule()` | ✅ | — |
| `fbref_player_stats` | `read_player_season_stats('standard')` | ✅ | — |
| `fbref_player_stats_extended` | merge 4× `read_player_season_stats({standard,shooting,playing_time,misc})` | ⚠️ | custom merge в нашем коде |
| `fbref_team_stats` | `read_team_season_stats('standard')` | ✅ | — |
| `fbref_team_stats_extended` | merge 4× `read_team_season_stats(...)` | ⚠️ | custom merge |
| `fbref_keeper_stats` | `read_player_season_stats('keeper')` | ⚠️ | нет `keeper_adv` (наш scraper мерджит keeper+keeper_adv) |
| `fbref_player_match_stats` | `read_player_match_stats('summary')` | ✅ | (dead upstream Feb 2026, всё равно) |
| `fbref_shot_events` | — | ❌ | нет метода (dead upstream — neutral) |
| `fbref_match_events` | `read_events()` | ✅ | — |
| `fbref_lineups` | `read_lineup()` | ✅ | — |
| `fbref_team_match_stats` | `read_team_match_stats('schedule')` | ✅ | — |
| `fbref_match_managers` | — | ❌ | **наш scorebox parser, Phase 1.5 dim_manager — upstream НЕ покрывает** |

**Поддерживаемые `stat_type`** в 1.9.0 (hardcoded):
- `read_player_season_stats`: `{standard, keeper, shooting, playing_time, misc}` (5 типов)
- `read_team_season_stats`: те же 5
- `read_team_match_stats`: `{schedule, shooting, keeper, misc}` (4 типа)
- `read_player_match_stats`: `{summary, keepers}` (2 типа)

Это фактически совпадает с тем, что у нас «живо» после FBref Feb 2026 restriction. **Coverage сам по себе НЕ блокирует миграцию.**

## 4. Phase 4 — Cloudflare bypass + proxy compatibility

Главный пробник: `https://fbref.com/en/comps/9/Premier-League-Stats` (та же стартовая страница, что `read_schedule`).

### 4.1 Out-of-the-box (без proxy)

Прямой вызов `read_schedule()` в контейнере → 5 retry × увеличенные паузы → `ConnectionError: Could not download https://fbref.com/en/comps/`.

Детали из `_validate_page`:
```python
timeout = 15  # seconds — hardcoded
while time.time() - start < timeout:
    if "<table" in self._driver.page_source:
        return ...
    time.sleep(0.5)
raise Exception("Could not retrieve page content within timeout. "
                "Possible reasons: failed CAPTCHA, IP block or network issues.")
```

15 секунд жёстко зашиты в код, не подкручиваются параметром. Наш cf-verify тратит ~8-30s на разных доменах.

### 4.2 Passive seleniumbase UC mode (без gui_click_captcha)

```
reconnect=4s, no proxy → title='Just a moment...' (CF challenge held)
reconnect=15s, no proxy → still 'Just a moment...' даже после второго driver.reconnect(8)
reconnect=25s, no proxy → still 'Just a moment...'
```

Passive UC mode (без активных кликов по Turnstile-капче) **не пробивает CF на fbref.com** в нашем окружении.

### 4.3 Активный bypass — `uc_gui_click_captcha`

```
NOTE: You must install tkinter on Linux to use MouseInfo.
Run the following: sudo apt-get install python3-tk python3-dev
```

`python3-tk` **недоступен** в нашем airflow image (`apt-get install python3-tk` → `Package has no installation candidate`, apt-repo заблокирован/неполный). Установка требует **rebuild docker image** — это уже плюс ≥1 день setup + cascading rebuild downstream (scheduler/workers).

### 4.4 Proxy compatibility — broken для residential

С нашим residential proxy `http://user:pass@pool.proxys.io:10000`:
- Driver init: `Driver(uc=True, proxy=proxy_url, host_resolver_rules='MAP * ~NOTFOUND , EXCLUDE 127.0.0.1')`
- Результат: `ERR_NAME_NOT_RESOLVED` (Chrome neterror page, title='fbref.com', body содержит `<body class="neterror">`)
- Причина: `host_resolver_rules=MAP * ~NOTFOUND` блокирует **локальный DNS resolution** в пользу DNS-через-proxy. Наш residential pool отдаёт CONNECT, но DNS-via-proxy в этом setup не отрабатывает.

Workaround = пропатчить `_init_webdriver` (переопределить `resolver_rules=None`), что ломает гарантии soccerdata про IP-leak — это уже не «free out-of-the-box», а custom downstream patch.

Дополнительно: **`soccerdata.FBref` принимает только один proxy** (str / list / callable) → нет нашей логики 1000-proxy ротации с `timeout_ban_threshold`, health check'ами и dead-proxy экcept-list'ом. Внутри `_init_webdriver` proxy строка резолвится один раз на driver lifetime; ротация = ребут driver'а (`sb.Driver.quit()` + `sb.Driver(...)`) — дорого.

### 4.5 Decision matrix (из issue body)

| Критерий | Свой scraper | soccerdata 1.9.0 | Победитель |
|---|---|---|---|
| mean s/match | ~9.67 (post-#57 fix) | **bench не получилось запустить** (см. 4.1-4.4) | свой |
| success rate | 100% (10/10 на baseline bench) | 0% out-of-the-box | свой |
| coverage (наши 11 Bronze) | 100% | ~80% (нет `fbref_match_managers`, `*_extended` merge нужен) | свой |
| Maintenance LOC | ~5680 + 4146 (bypass) = ~9826 | ~150 (wrapper) + downstream patches на proxy/UC mode | soccerdata |
| Proxy rotation | 1000 residential, health-tracking | single proxy, без ротации | свой |
| CF Turnstile bypass | cf-verify (passive, work, 100% success) | seleniumbase UC + `uc_gui_click_captcha` (требует tkinter, image rebuild) | свой |
| Lock-in risk | мы могём всё | tight coupling на soccerdata + seleniumbase upstream | свой |

Результат: **6/7 критериев** в пользу нашего скрапера. Только LOC-метрика в пользу soccerdata, но при учёте downstream patches (custom resolver_rules patch, custom proxy rotation adapter, post-process для `*_extended` + `fbref_match_managers`, image rebuild для tkinter) реальная экономия LOC сжимается с ~9800 до примерно **~3000-4000**, и за это мы платим стабильностью.

## 5. Recommendation: **HOLD** (do not migrate)

Soccerdata 1.9.0 BaseSeleniumReader на текущем поколении (релиз 2026-04) **не готов** заменить наш FBref scraper. Главные блокеры — не coverage (там как раз почти ОК), а **CF bypass + proxy story**:

1. **Активный Turnstile bypass требует tkinter** → docker image rebuild + cascading deploy. Минимум +1 день setup.
2. **Passive UC mode не пробивает fbref.com CF** в нашем окружении (3 probe variants всё time-out на «Just a moment...»).
3. **`host_resolver_rules` ломает наши residential proxies** → нужен custom monkey-patch `_init_webdriver`.
4. **Single-proxy без ротации** → пришлось бы оборачивать в наш ProxyManager на уровне driver-quit/init, ~отказ от бенчика реальной выгоды по simplification.
5. **15s `_validate_page` timeout жёстко зашит** — не подкручивается параметром, наш cf-verify иногда тратит 20-30s на retry-flow.

### Что менять в подходе

- **Не двигаться к 1.9.0 сейчас.** Bump `soccerdata>=1.8.8` → закрываем, иначе breaking import для прочих consumers (FotMob/Understat/SoFIFA — они не FBref-specific, но FBref backend в 1.9.0 теперь breaking-changes).
- **Зафиксировать `soccerdata==1.8.8`** (точечный pin, не `>=`) в `requirements-scraping.txt` чтобы избежать accidental upgrade на 1.9.x при пересборке.
- **Issues #65 (HTTP fast-path) не закрывать** — он остаётся актуальным как самостоятельный optimization для нашего nodriver pipeline.
- **Issues #52, #57** уже закрыты — связанный pain остаётся, но это не делает миграцию выгодной.

### Когда переоценить

- soccerdata 2.x с overridable `_validate_page` timeout + native CF-verify-like passive bypass
- Drop поддержки `uc_gui_click_captcha` в пользу passive UC (если seleniumbase upstream сделает Turnstile passive solve работающим в headless)
- Если у нас появится альтернативный proxy provider с DNS-via-proxy совместимостью с `host_resolver_rules`

В обоих случаях — переоткрыть issue #67, повторить Phase 4 probes.

## 6. Артефакты пробников

В контейнере `airflow-webserver` (disposable):
- `/tmp/sd19_probe.py` — UC mode + gui_click probe (вылетает на tkinter)
- `/tmp/sd19_probe2.py` — passive UC reconnect probe (D/E/F variants)
- `/tmp/sd19_probe3.py` — page_source forensics после bypass

Эти файлы созданы для одноразового probing, не закоммичены — не входят в репозиторий.
