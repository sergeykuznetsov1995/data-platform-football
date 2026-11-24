# FotMob heatmap: автономный контракт Bronze

Offline-кандидат #2257 включает чистое ядро `scrapers/fotmob/heatmaps.py`,
адаптер `heatmap_service.py`, вызов из `service.py` после commit родительского
матча и контракты двух Bronze-таблиц. DAG, расписания, доставка и production
не изменены; текущее состояние в матрице остаётся **raw_only** до отдельной
доставки и приёмки. Silver/Gold в объём не входят.

Санитизированный probe матча 4813626: 29 игроков, 1 345 кругов, остатка нет,
viewBox `0 0 105 68`; SHA-256 декодированного тела:
`44e20d4e4a9ea37e3cfe12bf81be873285dc780357563e320ac254893ae92a35`.
Это свидетельство формы источника, не production-фикстура и не результат
запуска тестов этого пакета.

## Идентичность и API

`extract_heatmap_reference(parent, match_id)` сначала проверяет
`general.matchId`. `build_heatmap_reference(match_id, source_url)` принимает
только `https://pub.fotmob.com[:443]/prod/db/api/heatmap/match/<match_id>`
без credentials, query, fragment и нормализации пути. ID положительный,
десятичный, без ведущих нулей в URL. Непустая ссылка другого типа — ошибка.
Отсутствие `content` или самого поля, `None`/пустая строка в поле — отсутствие
URL; неверный тип `content` — ошибка формы.

Прямого запроса к pub URL нет. Wrapper строится единственным правилом:
`canonicalize_target("heatmap/match/<id>/heatmaps", {"heatmapUrl": source_url})`.
Исходная строка сохраняется без изменения. `target_key` — идентичность
канонического wrapper URL; `content_hash` — SHA-256 декодированных raw bytes.
Это разные идентичности: один target может иметь несколько версий тела.

`AssetMetadata` явно передаёт reference, original `observed_at`, hash, raw URI,
необязательные competition/season и HTTP Last-Modified.
`parse_heatmap(body, metadata)` сверяет hash и wrapper identity, затем проверяет
всю структуру и только после этого преобразует числа. Функции не выполняют I/O.
`source_last_modified` берётся из JSON `lastModified`, отдельно от HTTP заголовка.

В `fotmob_heatmap_assets` физическое наблюдение включает матч, `target_key`,
`content_hash`, `observed_at` и scope; текущий natural key — `match_id`.
В `fotmob_heatmap_points` физическое наблюдение включает hash содержимого asset,
`player_key`/`player_id` и `point_index`; текущий natural key —
`(match_id, player_id, point_index)`.
Обе таблицы партиционированы по `competition_id`/`source_season_key` и используют
`target_type=heatmap`, `entity_id=match_id` для замены сущности последним успешным
наблюдением, в том числе при ротации URL.
Порядок игроков и кругов исходный; одинаковые круги остаются отдельными строками.
Пустые игроки входят в `player_count`.

### Identity игроков (#2367)

`fotmob_heatmap_points.player_id` — legacy numeric suffix исходного `player_key`,
а не подтверждённый FotMob player ID. Старые точки, их natural key и batch identity
не меняются. Для связи с игроком используется отдельная таблица
`fotmob_heatmap_player_identities` с natural key `(match_id, player_key)` внутри
полного identity-снимка. Схема таблицы фиксирована, чтобы первый снимок только
с `not_attempted` не закрепил типы nullable-колонок через all-NULL inference.
Игрок с пустым фрагментом получает identity-строку без point rows; строки parent
без ключа в heatmap не создаются.

Resolver `player_stats_opta_id_v1` сравнивает digit-string после `p` со
`str(playerStats.optaId)` того же матча; ведущие нули значимы. Fallback по FotMob
ID, имени, команде или порядку записей запрещён. `match_status` принимает
`verified`/`mismatch`/`unverified`, `identity_status` —
`matched`/`missing`/`ambiguous`/`not_attempted`. При выполненном поиске 0/1/2+
кандидатов даёт `missing`/`matched`/`ambiguous` и соответствующий
`candidate_count`. Только `matched` получает `fotmob_player_id` из той же parent
записи. `identity_basis` фиксирует основание/причину,
`candidate_fotmob_player_ids_json` — численно отсортированный массив кандидатов,
`NULL` при `not_attempted`. Missing/ambiguous/not_attempted не создают
произвольную связь и не удаляют исходные точки.

`heatmap_batch_id` связывает identity с неизменным legacy batch. Отдельный
`target_type=heatmap_identity` и synthetic target key не смешивают dedup двух
типов. Identity batch зависит от версии resolver, parent content hash и heatmap
batch, поэтому смена parent при неизменном heatmap обновляет только identity.
Отсутствие parent raw provenance не создаёт identity batch. Current выбирает целый
последний успешный identity-снимок по `(target_type, entity_id)`, затем применяет
natural key; исчезнувшие ключи не воскресают. Обогащать точки можно только по
`match_id`, дословному `player_key`, равенству `points._target_batch_id =
identity.heatmap_batch_id` и равенству `identity.parent_content_hash` хешу текущего
parent payload. При сбое новой identity прежняя версия другого parent не
подставляется: условие по parent hash отсекает её даже при неизменном heatmap batch.

Ошибка identity commit не меняет уже успешный parent/heatmap outcome и не
дублирует их строки. Повтор той же parent+heatmap пары сохраняет оба batch ID и
не создаёт новых physical rows. Откат кода не требует удаления side table или
миграции points: прежний reader игнорирует identity и продолжает читать старые
heatmap tables/batches. Само production-создание таблицы — отдельный разрешённый шаг.

Offline evidence 24.11: после разрешения владельца из raw-хранилища прочитаны
ровно шесть объектов одного завершённого run `fotmob-20261118T150425.516799Z`:
parent и heatmap матчей 5181902, 5181903, 5181930. Новых запросов к FotMob HTTP
не было; Trino query `20261124_165358_83421_dyben`. Независимый stdlib-разбор
проверил parent match ID, heatmap wrapper match ID, content hashes и исходные
player keys. По трём полным raw-парам получено 94 ключа / 4341 точка:
`matched=94`, `missing=0`, `ambiguous=0` по `playerStats.optaId`; по FotMob `id` —
`matched=0`, `missing=94`, `ambiguous=0`. Во всех 94 matched Opta и FotMob ID
различаются. Например `p194827` означает Opta `194827`, но FotMob `566753`
(Rey Manaj); `p517997` — Opta `517997`, FotMob `1208969` (Kristjan Asllani);
`p61824` — Opta `61824`, FotMob `150747` (Etrit Berisha). Кандидат повторил все
94 связи, не изменил ни одной исходной точки и сохранил unknown semantics.
Воспроизводимые inputs, скрипт и детерминированные outputs находятся в
`.local/tasks/2367-heatmap-identity-evidence/authorized-raw-pairs/`;
`raw_pair_verified=true`. Это доказательство для проверенного формата
`p<digits>`, а не утверждение о других возможных форматах ключей.

Template должен быть корректным SVG с конечным четырёхчисловым viewBox и
положительными width/height. Сохраняется точная исходная запись атрибута viewBox,
включая пробельные символы и character references до XML-нормализации; числа
проверяются по значению ElementTree. Геометрия template
не превращается в точки. Players — объект `p<положительный ID>` → XML-фрагмент.
Фрагменты допускают только непосредственные circle с ровно `cx`, `cy`, `r`;
пробельное форматирование разрешено, содержательный текст, комментарии, PI,
вложенные элементы, DTD и лишние атрибуты запрещены. JSON с повторными ключами
отклоняется. Значения координат должны конвертироваться в конечные float;
выход за viewBox не отфильтровывается. Единицы — `svg_view_box_user_units`;
orientation и radius_semantics — **unknown**. Футбольное направление и физический
смысл радиуса не выводятся из формы SVG.

## Исходы и граница manifest

| Внешний outcome | Значение |
| --- | --- |
| `url_missing` | Валидный родитель не содержит ссылки |
| `resource_unavailable` | Scoped NOT_AVAILABLE с 204/404 либо HTTP 200 с проверенным JSON null |
| `unexpected_format` | Неверная идентичность/URL, JSON, SVG, viewBox или структура players |
| `not_applicable` | Валидная структура, суммарно ноль кругов; asset есть, точек нет |
| `success` | Непустая полностью типизированная проекция |
| `parse_failed` | Структура полностью проверена, но cx/cy/r не конечный float |
| `unknown` | Transient/stale, неопределённый terminal/network/raw-store результат или несовпадение hash |

`classify_heatmap_fetch` возвращает либо разрешение разбора тела, либо terminal
outcome. SUCCESS и NOT_MODIFIED с телом разбираются; STALE_REPLAY не считается
доказательством. Terminal HTTP 200 с доказанной ошибкой JSON/decode — unexpected_format;
отсутствующее тело само по себе остаётся unknown, поскольку возможно повреждение
raw-store. Прочие terminal состояния не доказывают отсутствие ресурса.
Вызывать классификатор следует только для проверенного wrapper конкретного матча.

Адаптер сохраняет семь внешних исходов и переводит их в статусы manifest:

- `success`/`not_applicable` → `success`/`not-modified` с asset и points;
  при нуле точек публикуется только asset.
- `resource_unavailable`, а также `url_missing` после прежнего успеха →
  tombstone `NOT_AVAILABLE`. Первый `url_missing` остаётся только в отчёте;
  исчезновение URL после успеха создаёт tombstone с опорой на raw родителя.
- `unexpected_format`/`parse_failed` → `SCHEMA_DRIFT`, без строк.
- `unknown` → `RETRYABLE_FAILURE`, без строк, если есть `FetchResult`.

Ядро не меняет manifest schema и не приравнивает unknown к отсутствию. Любая
ошибка разбора возвращает **ноль asset и point rows**; частичная публикация запрещена.

## Service adapter

Helper запускается только после успешного commit родительского матча: payload
родителя остаётся опубликованным независимо от результата heatmap. Ошибки
дочерней операции доступны через operation errors/retryable/report; родитель
повторно не фиксируется как failed.

Live-загрузка heatmap использует только разрешённый wrapper, никогда прямой
pub URL. Действуют общий ledger запросов/байтов и совокупная late-квота запросов
родителя и heatmap; cutoff проверяется перед каждой heatmap HTTP-попыткой.
Явный replay выполняет ноль HTTP-запросов и сохраняет исходный `observed_at`.

Без CAS `content_hash`, `raw_uri` или `observed_at` typed-публикация невозможна.
Детерминированный batch исключает дубли физических строк при идентичном replay.
Полный отчёт находится в `OperationResult.metadata["heatmap_report"]`;
deferred-диагностика хранится отдельно. Если writer падает после успешного
разбора, исход отчёта заменяется на `unknown`, чтобы не сообщать ложный typed-успех.

## Replay и отчёт одного прогона

CAS сохраняет immutable blob по content_hash; обновление того же canonical URL
с v1 на v2 меняет latest pointer, но сохраняет обе версии. Для воспроизведения
нужно передать выбранное observation с target_key, content_hash, raw_uri,
fetched_at, validated_at и canonical_url в capabilities_json. Replay выбранного
hash не должен подменяться чтением latest. Original observed_at передаётся из
выбранного наблюдения. Explicit offline replay даёт SUCCESS без HTTP и без
HTTP Last-Modified; source lastModified восстанавливается из тела.

`HeatmapObservation` содержит явные evidence flags. `observation_from_parse`
строит запись из результата парсера; origin задаёт адаптер. Для ранних исходов
адаптер создаёт запись непосредственно. `aggregate_heatmap_report` принимает
ровно один run_id, удаляет только полностью одинаковые записи одного матча,
отклоняет противоречия outcome/hash/origin/счётчиков и повтор target у другого матча.
Порядок входа на результат не влияет; пустой отчёт содержит все нулевые счётчики.

| Счётчик | Определение |
| --- | --- |
| `matches` | Уникальные матчи после дедупликации |
| `matches_with_url` | Матчи с явным has_url, включая ещё не загруженные маркеры |
| `unique_assets` | Различные wrapper target_key, реально переданные transport/CAS |
| `fetched` | Различные target_key с hash-verified телом из HTTP или CAS |
| `network_fetched` | Компонента fetched от сетевого пути, включая 304 с телом из CAS |
| `replayed` | Компонента fetched от явного offline replay; без пересечения с network |
| `decoded` | Полностью проверены JSON/SVG/players; включает числовой parse_failed |
| `typed` | Успешные непустые typed assets |
| `typed_rows` | Сумма точек только успешных assets |
| `unavailable` | Только доказанный resource_unavailable |
| `unknown` | Число неопределённых terminal исходов |
| Все семь имён outcome | Число матчей с соответствующим исходом |
| `outcomes` | Те же семь счётчиков во вложенном объекте |

Проверяется `typed ≤ decoded ≤ fetched ≤ unique_assets ≤ matches_with_url ≤ matches`;
`network_fetched + replayed = fetched`. URL-маркер сам по себе не увеличивает
unique_assets/fetched. Успешный asset требует хотя бы одну точку; другие исходы
не имеют typed_rows. Все матчи имеют ровно один исход, включая not_applicable.

## Offline-проверка и оставшиеся шаги

Ведущий запускает focused-тесты ядра, service, repository и replay, затем
независимое ревью DeepSeek. Модели тесты не запускают. Полный unit suite явно
отложен до завершения #2256. В этой offline-фазе нет Trino, live HTTP, доставки
или merge.

Следующие шаги требуют отдельного разрешения: полный unit suite,
commit/push/PR/обязательные CI/merge, доставка, затем ограниченный live-пилот
ровно на трёх разных матчах с `heatmapUrl`. Для каждого матча количества
и выбранные точки должны совпасть с raw, а replay — дать идентичный результат.
Production-приёмка по критериям #2257/#2258 и закрытие issue остаются отдельными
шагами; этот offline-кандидат их не подтверждает.

После отдельной доставки нужны разрешённые live-проверки wrapper/отсутствия
ресурса, сохранения raw версий, scoped asset/point записи, отчёта и фактического
состояния Airflow; приёмка по актуальным критериям #2257/#2258 остаётся отдельной.
