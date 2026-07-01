# Transfermarkt Proxy Traffic Audit — squad-first crawl

> Branch `feat/transfermarkt-traffic-review` · 2026-07-01
>
> **Вопрос:** Transfermarkt — главный потребитель residential-прокси платформы
> (~$4/GB). Куда уходят мегабайты и как их убрать, не потеряв ни одной таблицы?

## TL;DR

1. **Baseline (`iceberg.ops.proxy_traffic_runs`, июнь 2026): Transfermarkt съел
   1168 MB — ~95% всего residential-трафика платформы** (fbref 27 MB, capology
   5.6 MB). Недельный запуск ≈ 66 MB, из них таск `scrape_players` — **58.2 MB
   при ~551 запросе**.
2. Причина: `read_players` после 20 squad-страниц докачивал **~530 профилей
   игроков**, хотя детальная squad-таблица `/plus/1` уже содержит все bio-поля
   (position, DOB/age, nationality, height, foot, а для TM-текущего сезона — и
   Contract). Подтверждено живым probe 2026-07-01.
3. **Fix (squad-first): профильный цикл удалён.** Парсер squad-страницы стал
   header-driven (набор колонок различается между TM-текущим и прошлыми
   сезонами: `Contract` vs `Current club`).
   Live dry-run 2026-07-01: **21 запрос, 3.72 MB, 798 строк** (полнее прежних
   555 — постсезонный вид отдаёт всех игроков сезона). **−94% MB, −96%
   запросов** на players-таск; недельный запуск ~66 → **~8-10 MB**; бэкфилл
   сезона 58 → ~4 MB.
4. **Июльское окно (активно прямо сейчас):** TM переключает «текущий» сезон в
   начале июля, наш `CURRENT_SEASON` — в августе. В этом окне колонки Contract
   на сезонной странице нет → `contract_until` переносится из существующей
   bronze-партиции (carry-forward; live dry-run: 570/798 строк заполнено из
   bronze). `market_value_last_update` больше не скрейпится вовсе — Silver
   выводит `mv_last_update` из `bronze.transfermarkt_market_value_history`
   (MAX(mv_date) per player) с COALESCE для старых строк.
5. **Coaches: профили только для новых тренеров.** Bio (dob/nationality)
   неизменяемы — переиспользуются из bronze (любой сезон); недельный запуск
   качает listing + 20 history-страниц + профили только новых назначений:
   ~6 → **~3-4 MB**.
6. **Traffic-guard kill-switch** (зеркало FBref-guard'а): таск
   `check_traffic_guard` читает per-entity result-JSON
   (`traffic.proxy_response_mb`, #789) и валит run при превышении порога
   (Variable `tm_proxy_mb_threshold_<entity>` → `tm_proxy_mb_threshold` →
   дефолты players 10 / mvh 4 / transfers 8 / coaches 6 MB). Параллельный
   лист — красный run не блокирует промоушен уже оплаченных данных в Silver.
7. `mv_history`/`transfers` уже были оптимальны (ceapi JSON, 0.44 + 1.21 MB на
   окно в 100 игроков) — не тронуты.

## Baseline (до фикса)

| Таск | Запросы | MB/запуск (декомпресс. body) |
|---|---|---|
| scrape_players | ~551 (1 listing + 20 squads + ~530 profiles) | 58.21 |
| scrape_coaches | ~41-61 (1 listing + 20 history + ~20-40 profiles) | 5.97 |
| scrape_market_value_history (окно 100) | ~100 | 0.44 |
| scrape_transfers (окно 100) | ~100 | 1.21 |
| **Итого / неделя** | **~800** | **~66** |

Июнь 2026 суммарно (со всеми бэкфиллами/ручными запусками): **1168 MB / 41 запусков**.

## After (live-верификация 2026-07-01)

| Проверка | Результат |
|---|---|
| Dry-run `--entity players --season 2025` | **21 запрос, 3.72 MB, 798 строк (781 уникальных)** |
| Полнота полей (live squad-страница, Arsenal 25/26, 40 строк) | position/dob/age/nationality 100%; height 35/40, foot 34/40, MV 34/40 (юниоры без данных — как и на профилях) |
| contract carry-forward | 570/798 строк заполнено из bronze (остальные — новые для bronze игроки постсезонного вида) |
| Unit-тесты | 136 passed (scraper/runner/guard/SQL) + sqlglot alignment 59 passed |

**Известный trade-off:** у хвостовых игроков (юниоры, редкие выходы) squad-таблица
иногда не заполняет height/foot/MV, которые профиль отдавал (~99% → ~87%
non-null по этим полям на клуб). Для gold `dim_player_attributes`
(MAX_BY(season) enrichment основных игроков) некритично; при необходимости
можно добить точечным профильным fetch'ем только строк с NULL-bio (~30-60
запросов вместо 530) — осознанно НЕ реализовано (YAGNI).

## Не сделано / follow-ups

- **Байтовая метрика считает декомпрессированный body** (`len(resp.content)`),
  биллинг же идёт по wire-байтам (сжатый body + TLS-хендшейки; клиент
  пересоздаётся на каждый запрос — свой хендшейк на каждый URL). Относительные
  сравнения валидны, абсолютные MB — верхняя оценка body и нижняя транспорта.
- Retry ×3 на 403 без backoff и без circuit breaker: каскад ограничен
  `ConsecutiveFailureError(50)` и теперь traffic-guard'ом постфактум.
- `_validate_bronze_quality` получил WARNING-only roster-coverage проверки
  (#620): доля игроков сезона с mvh/transfers-строками ≥ 90% (live сейчас:
  mvh 99.8% текущего сезона).
