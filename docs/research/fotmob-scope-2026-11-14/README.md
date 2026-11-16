# FotMob #2258: scope и capability/freshness

Research-пакет фиксирует область Bronze и контракт возможностей по семействам. [scope-registry.csv](scope-registry.csv) содержит решения и exact source season keys; [capability-freshness-matrix.csv](capability-freshness-matrix.csv) разделяет CURRENT и TARGET; [profile-transitions.json](profile-transitions.json) объясняет смены season schema; [validation.json](validation.json) хранит детерминированные проверки registry и matrix. Матрица опирается на `scrapers/fotmob/repository.py`, `parsers.py`, `service.py`, `match_refresh.py`, `transfers.py`, `transfer_contract.py` и [матрицу проекций](../../operations/fotmob-field-projection-matrix.md).

Provenance: live `allLeagues` от 2026-11-14 — 557 уникальных IDs, 567 occurrences, SHA-256 `6cfddd007ca90c78bf412c885b06241167eda01dbcf4fa1b8459fe799cdf3067`. Решения взяты из stored identity-matching profiles: это не новый массовый live probe. Catalog fingerprints совпали у 557/557 IDs. Отдельные read-only Trino/S3 доказательства дают сезонные строки и hash-verified raw profiles; production не менялся. Новых live probes к FotMob не было; лид выполнял read-only запросы и детерминированные проверки. Независимый анализ GLM `glm-5.3/high`: run `fab0267d439f4be48f5b4e7d294296ed`; формула и golden-примеры приняты лидом. Astra `gpt-6-astra/high`, run `12aea3e71526480bbe7abd5e52d64da8`, заполнила README и matrix по встроенным доказательствам.

Область: 452 included + 105 excluded. Включены adult men's competitions, включая **friendlies**; women, youth/U23, reserve и show исключены. Policy version — `fotmob-owner-scope-2026-11-14-v2-friendlies-included`, classifier — `fotmob-men-v1`. Каталог служит discovery: fan-out требует identity-matching profile и структурного решения, одного названия недостаточно. Контрольные примеры: Friendlies (114) и Club Friendlies (489) включены; Women's Friendlies (293) и Friendlies U21 (344) исключены.

Все 452 included имеют season evidence: 5 568 available rows, 5 530 пар со стартовым годом >=2010 и **5 540 required**. Формула для каждого competition: `первые 10 source_order ∪ start>=2010`; если источник объявил меньше десяти ключей, отсутствующие не добавляются. Порядок берётся из source evidence, exact labels сохраняются. Это даёт доступную историю до 2010/11 и последние десять доступных source scopes, в том числе более старые для редких турниров.

Ровно 10 дополнительных required pre-2010 keys принадлежат EURO и World Cup; это не дубли:

- EURO (50): последние десять — `2028|2024|2020|2016|2012|2008|2004|2000|1996|1992`; добавлены пять `2008|2004|2000|1996|1992`.
- World Cup (77): последние десять — `2026|2022|2018|2014|2010|2006|2002|1998|1994|1990`; добавлены пять `2006|2002|1998|1994|1990`.

Всего available pre-2010 — 38; из них 28 не required: EURO — 8, Allsvenskan — только `2002`, World Cup — 13, по одному `2009/2010` у IDs 121/139/166/222 и Soccer Aid — `2006|2008`. Полные exact keys находятся в registry. Арифметика: `5 568 − 5 530 = 38`, `38 − 28 = 10`, `5 530 + 10 = 5 540`.

Три multi-latest строки имеют статус `present_profile_transition`, а не unknown/review. Hash-verified raw profiles показывают следующие переходы:

- 10201: latest/selected `2025/2026` сменился на `2026`; исторические `2021/2022`, `2016/2017`, `2012/2013` сохранены. Это переименование к annual label.
- 112: annual latest `2026` сменился на `2026 - Clausura`; появились Apertura/Clausura и исторические Final/Inicial labels. Annual `2026` и `2025` также остаются в текущем объявлении; часть прежних annual labels отсутствует в нём.
- 199: annual latest `2026` сменился на `2026 - Clausura`; объявлены Apertura/Clausura для 2010–2026, вместе с annual `2026` и `2025`; прежние annual `2010`–`2024` отсутствуют в текущем объявлении.

Latest identity-matching profile задаёт current declaration; union сохраняет все exact raw scopes для replay. Совпадающие годы и разные labels не доказывают разные футбольные розыгрыши. Edition reconciliation по match/scope IDs принадлежит #2255; для #2258 новые live probes не нужны. Отдельно у 56 строк scope profile hash отличается от season payload hashes: это разные поколения доказательств, не дефект и не автоматическое основание для re-probe.

CURRENT в матрице описывает существующую проекцию семейства. `typed` означает конкретные колонки, включая JSON fragments: например, `lineup_json` не является полностью нормализованной моделью состава. Полный футбольный raw JSON сохраняется наряду с проекциями. TARGET для heatmaps — `typed` по #2257; CURRENT — `raw_only` для ссылки `content.heatmapUrl`, collector самих heatmaps пока отсутствует. Grain будущих typed heatmaps определит #2257, текущий grain ссылки — родительский match payload. `h2h`, `attackingZones`, `weather` остаются `raw_only` и в CURRENT, и в TARGET по решению владельца. Silver — отдельная задача.

Freshness — контракт из кода и поручения, не новое измерение production. Для discovery/season sync отдельного численного SLO в предоставленном коде нет; дочерним семействам он не выдумывается. Profile review — 30d либо изменение fingerprint; leaderboard — порог повторного сбора 72h; team и squad — 48h; player — `once_per_identity`, новая identity собирается сразу. Transfers: max age 48h, окно `1year`, pagination `unique_hits`, cohort `included_ids`; `daily_complete` требует отдельных timestamp/flush/budget доказательств. Ни daily, ни green здесь не подтверждаются.

Основной C1 из #1277/#1896 — <=24h после окончания матча. Код использует settle margin 3h от kickoff и обозначает `T = kickoff + 3h`; late obligations наступают T+6h/T+48h, cutoff поздних HTTP — T+72h. Учитываются успешные source observations: позднее первое наблюдение может закрыть оба порога, ошибки попыток их не закрывают. Recovery уже полученного ответа допустим после cutoff. Это политика match payload, а не отдельный SLO каждой секции или гарантия свежести календаря. Seasons и matches могут изменяться; историческая дата не делает их неизменяемыми.

Allowed outcomes в CSV — словарь research-контракта. `source_unavailable` требует свидетельства отсутствия capability у источника; `not_applicable` — неприменимости, а не сетевой ошибки. `advertised_not_started` / `not_applicable` предусмотрены для future advertised seasons, например EURO 2028: ожидаемо пустой календарь не является ошибкой сбора и не требует выдумывать матчи. Сетевой сбой, parser error и один marker сами по себе не подтверждают успешный сбор. Marker/source key не равен уникальному футбольному розыгрышу; #2256 проверяет content против markers.

## Детерминированная проверка

[validation.json](validation.json) имеет schema `fotmob.scope-deliverable-validation.v1` и `status=PASS`:

- 557 строк и 557 distinct competition IDs; 452 included, 105 excluded; 557 совпадающих live catalog fingerprints; policy и probe counters совпали.
- Все 452 included имеют required seasons; available — 5 568, since-2010 — 5 530, required — 5 540. Golden EURO/World Cup подтверждены; 38 pre-2010 available и 28 not required сходятся.
- `required = первые 10 source_order ∪ start>=2010` проверено поэлементно; required unique и subset of available.
- `pending_probe=0`, `unparsed=0`, `unexplained_review=0`; три profile transitions объяснены; 56 differing hashes описаны как отдельные поколения доказательств.
- Все 23 семейства имеют grain, current/target representation, requiredness, freshness, allowed outcomes и historical applicability. Heatmaps current=`raw_only`, target=`typed`; future `advertised_not_started` присутствует.

Эти результаты относятся к registry и matrix. Пакет не является runtime/production acceptance, не подтверждает полноту содержимого всех scopes и не разрешает production-запуски или доставку.
