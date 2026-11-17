# FotMob #2256 — сверка completion-маркеров с match ID и секциями карточек

На замороженном срезе основная когорта #2258 содержит **563878 finished scoped ID**, из них **555006 с current payload** и **8872 без current payload** по буквальному правилу #1294. Применимый разрыв — **8825**. Из **7803** последних v2-маркеров только **431** получил подтверждение содержимым; **6898 unknown не засчитаны собранными**. Источник чисел: `evidence-report-build.md`, `evidence-marker-summary.json`.

Это результат read-only аудита Bronze, а не подтверждение полноты источника. По переданному контексту выполнены два прохода, verify и build; build сообщает Coverage PASS и Replay PASS. Документирующая фаза не запускала запросы, тесты или команды и не меняла production. Противоречие между временем прохода и его лимитом разобрано ниже; до его разрешения нельзя безоговорочно утверждать выполнение критерия бюджета.

## Метод и фиксированный срез

Единица scoped ID — match ID внутри пары competition/season. Один глобальный ID может присутствовать в нескольких scopes и иметь payload только в части из них. Основная когорта — required #2258; supplementary — объединение required, marker-only и исторических scopes. Флаги когорт пересекаются, поэтому их нельзя складывать. `scope-summary.csv` содержит каждый физический scope однократно.

Current-equivalent означает результат восстановления current на зафиксированных физических снимках Iceberg; это не чтение live current views. Ограниченные выборки календаря, manifest, C1, истории и payload сопоставляются наборами ID. Слепой full join `matches_current × match_payloads_current` не используется. Состав и SQL процедуры указаны в [README аудита](../../../scripts/research/fotmob_completion_audit/README.md); обзор ограничений запросов — в [queries.sql](queries.sql).

Снимки и время, когда они стали current. Источник таблицы: `evidence-snapshots.json`, совпадающие значения в `evidence-report-build.md`.

| Вход | Snapshot ID | Made current at, UTC |
| --- | --- | --- |
| c1 | 2953056259902045724 | 2026-11-15 20:09:50.847000+00:00 |
| manifest | 3196714190286422442 | 2026-11-14 14:31:55.609000+00:00 |
| matches | 3224980456284593238 | 2026-11-14 20:32:55.991000+00:00 |
| payloads | 153801965908223392 | 2026-11-14 13:29:02.835000+00:00 |

Применимость future вычисляется по `utc_time + 3 hours` относительно единственного frozen `captured_at = 2026-11-16T18:24:44.943480+00:00`, а не относительно времени повторного запуска (источник: build).

## Текущий знаменатель и наборы ID

Источник таблицы: `evidence-report-build.md`. Все значения — scoped ID; «с payload» здесь означает finished с current payload, а не положительный вердикт по всем секциям.

| Когорта | Known | Finished | Finished с payload | Finished без payload, literal | Actionable gap |
| --- | ---: | ---: | ---: | ---: | ---: |
| Основная required #2258 | 603039 | 563878 | 555006 | 8872 | 8825 |
| Supplementary union | 606056 | 565058 | 556186 | 8872 | 8825 |

Для всех текущих календарных строк build отдельно сообщает **556188 scoped payload rows**. Глобальные distinct-наборы: **603080 known IDs**, **556188 current payload IDs**, **562171 finished IDs**, **8872 IDs с хотя бы одним scoped literal gap**. Эти значения нельзя подставлять в арифметику scoped-когорты. Число payload также не равно числу `confirmed_content:current_payload_present`: классификация учитывает применимость, а итог marker — ещё и секции.

Полные строки сохранены в [current-id-evidence.csv](/root/data-platform-football/.local/tasks/2256-completion-audit/run-pass-c/passes/pass1/current-id-evidence.csv), SHA256 `3ef238d46d71837edf51aa926066d8b6485159654d7d95a252f51be485205e34`. CSV сохраняет совпавшие и отсутствующие scoped ID, глобальные ID, batch identities, флаги и времена наблюдения. Из него процедура воспроизводит known finished, finished с payload и finished без payload с сохранением scope/cohort, а не только суммы. Сам отчёт не заменяет этот файл и весь frozen run.

Контрольные суммы наборов ниже перенесены буквально из `evidence-report-build.md`. Имена `payload_*` не переименованы в finished-with-payload: это разные определения. Отдельный SHA256 finished-набора во встроенном build не приведён; для него опорой остаётся полный CSV с указанным SHA256 и процедура выделения. Формат канонизации наборов следует брать из сохранённой процедуры, а не заменять произвольной сортировкой CSV.

| Набор | SHA256 |
| --- | --- |
| known_global | `2748d7fbf4c413cd11d5934f9960681e176d83ee29307998ca65bfba8789c9d2` |
| known_scoped | `0ea94bb97a6b739abb7f334d48db40d88bc383fa5147e76402ff89c98fee0842` |
| literal_gap_global | `0dd1fac1254b1deb0f27a5b1f58599282bcd5104929d0902ad7fa401c6f82a45` |
| literal_gap_scoped | `3e1eb0d4eaf5214c546650df55ed8c300364394136b56bff636e5e98beb95d1e` |
| payload_global | `43cb10a125cc37cc031ea3a0e485b6233ff7f7a79ced41dce981bc91d37d468b` |
| payload_scoped | `63d0b0528e706a23a78b2a7c4cecbf23f8c49d477a4b08763e2ddc33f618034f` |
| required_known_global | `9b0c6e8cc23a6b7cd227fb01d4068080b0c9828dcecf03f8e33fb87965bdf7db` |
| required_known_scoped | `d74ce4054acb81f721cc016551dbe044744ac59bfa8bf522829b8f41e61a754c` |
| required_literal_gap_global | `0dd1fac1254b1deb0f27a5b1f58599282bcd5104929d0902ad7fa401c6f82a45` |
| required_literal_gap_scoped | `3e1eb0d4eaf5214c546650df55ed8c300364394136b56bff636e5e98beb95d1e` |
| required_payload_global | `9555cf08d3bc031b411ce2928d355d505b1b04e0a14d6351eac2e009f0ee26d2` |
| required_payload_scoped | `b3598d91fdd5509b0045fff6a71b1b783e400871651bd28f0dc69dce0a6abed4` |

### Турнирная разбивка

Крупнейшие literal gaps supplementary union, по убыванию. Источник каждой строки: таблица `Supplementary competition breakdown` в `evidence-report-build.md`. Числа здесь — по ВСЕМ scopes турнира (например, 199 — 51 scope); исторические 18 пар #1294 из секции ниже — только подмножество сезонов того же турнира (у 199 — 14 из 51), поэтому суммы этих таблиц не совпадают и не должны. Это top-список, не полный знаменатель; полная разбивка остаётся в build и `scope-summary.csv`. Причины конкретных ID связываются с турниром и сезоном через `current-id-evidence.csv` и `mismatched-ids.csv`; агрегированные причины ниже не приписываются отдельным турнирам без этой связи.

| Competition ID | Турнир | Scopes | Known | Finished | С payload | Literal gap | Actionable gap |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 199 | Division Profesional | 51 | 4202 | 4142 | 2215 | 1927 | 1927 |
| 9039 | LPF | 11 | 1952 | 1946 | 735 | 1211 | 1211 |
| 112 | Liga Profesional | 30 | 4906 | 4853 | 3893 | 960 | 960 |
| 522 | Premier League | 12 | 2862 | 2646 | 1927 | 719 | 719 |
| 10626 | FA Cup Qualification | 17 | 3508 | 3496 | 2925 | 571 | 570 |
| 9066 | Premier League | 16 | 2750 | 2590 | 2042 | 548 | 548 |
| 9138 | Segunda Federación | 13 | 9435 | 8249 | 7810 | 439 | 439 |
| 134 | Coupe de France | 17 | 3717 | 3622 | 3279 | 343 | 343 |
| 10448 | Liga Revelação | 1 | 256 | 256 | 40 | 216 | 216 |
| 9372 | Azadegan League | 13 | 3114 | 3032 | 2886 | 146 | 146 |
| 9112 | Liga 3 | 17 | 6572 | 6007 | 5869 | 138 | 138 |
| 10628 | Norgesmesterskapet Kvalifisering | 16 | 1225 | 1144 | 1006 | 138 | 138 |
| 47 | Premier League | 17 | 6461 | 6181 | 6070 | 111 | 111 |
| 10608 | Africa Cup of Nations Qualification | 10 | 1246 | 1122 | 1014 | 108 | 108 |
| 9833 | Asian Games | 5 | 186 | 181 | 74 | 107 | 107 |
| 8870 | Championship | 16 | 737 | 707 | 611 | 96 | 96 |
| 9100 | 2. Division | 17 | 2423 | 2281 | 2191 | 90 | 90 |
| 63 | Premier League | 17 | 4136 | 3978 | 3898 | 80 | 80 |
| 9011 | Emperor's Cup | 17 | 816 | 806 | 738 | 68 | 68 |
| 9490 | Copa Colombia | 12 | 806 | 797 | 739 | 58 | 58 |

### Причины на уровне payload

Источник: `Payload reasons` в `evidence-report-build.md`. Это scoped rows полного набора классификаций, а не взаимоисключающие части finished-gap знаменателя. В отчёте внутренний токен `confirmed_content` отображается словарём issue как `confirmed_by_content`.

| Категория issue | Reason | Scoped rows |
| --- | --- | ---: |
| confirmed_by_content | current_payload_present | 555976 |
| not_applicable | cancelled | 17132 |
| not_applicable | future | 22234 |
| not_applicable | unfinished | 1889 |
| source_unavailable | explicit_scope_source_gap | 1242 |
| source_unavailable | repeated_source_data_not_found | 4149 |
| unknown | insufficient_absence_evidence | 547 |
| writer_failed | expected_current_payload_missing | 2887 |

`source_unavailable` опирается на явный scope gap либо повторные `data_not_found`, а не на простое отсутствие payload. `writer_failed:expected_current_payload_missing` выделяет ожидаемое содержимое, которого нет в current, в отдельную категорию. Название категории не устанавливает конкретную программную причину потери. `unknown:insufficient_absence_evidence` означает недостаточность доказательств; его нельзя объявить ни собранным, ни отсутствующим у источника.

## Вердикты всех v2-маркеров

Источник таблицы: `evidence-marker-summary.json`, подтверждающая таблица `Latest v2 markers` в build. Все **7803** latest targets имеют результат, включая прерванные последние строки. `fetch_failed` и `parse_failed` входят в допустимый словарь, но не представлены в переданном распределении первичных вердиктов; это не доказательство отсутствия таких попыток в истории.

| Вердикт словаря issue | Targets |
| --- | ---: |
| confirmed_by_content | 431 |
| source_unavailable | 325 |
| not_applicable | 116 |
| writer_failed | 33 |
| unknown | 6898 |

Полный словарь: `confirmed_by_content / source_unavailable / not_applicable / fetch_failed / parse_failed / writer_failed / unknown`. Внутренний `confirmed_content` — только техническое имя `confirmed_by_content`, а не дополнительная категория.

`marker-results.csv` сохраняет `competition_id`, `source_season_key`, `plan_signature`, `entity_id`, `cohort`, `target_key`, `batch_id`, `status`, `category`, `reason_counts`, `scope_keys`, `evidence_sha256`. Таким образом, результат связан с точным target и batch, а не только с турниром или статусом success.

Ведущий публикует `marker-verdicts.csv` детерминированной трансформацией `marker-results.csv`: внутренний токен `confirmed_content` отображается в `confirmed_by_content` в публикуемом словаре вердиктов. Идентичности targets, причины, scope keys и evidence hashes сохраняют связь с исходными строками; исходный файл сохраняется. SHA256 опубликованных байтов фиксируется при публикации. Встроенные материалы не содержат этот SHA256, поэтому здесь он не выдуман и публикация не объявляется выполненной документирующей фазой. CSV этой фазой не генерируется.

Marker-level результат — worst-of по причинам матчей и секций. Порядок приоритета от наиболее ограничивающего: `unknown`, `writer_failed`, `parse_failed`, `fetch_failed`, `source_unavailable`, `not_applicable`, `confirmed_by_content`. Любой unknown перекрывает положительные свидетельства внутри того же target. `source_unavailable` и `not_applicable` также не являются подтверждением содержимым. Обычные unfinished not-applicable строки исключаются из completion/marker totals, но сохраняются в полном локальном evidence.

Например, для `268 / 2021` marker имеет `status=success`, **380** current payload и **377** `unknown:momentum:malformed_or_unresolved_json`; первичный результат — `unknown`. Для `11648 / 2026` payload и все секции согласованы по **1** наблюдению, результат — `confirmed_by_content`. Для `166 / 2015/2016` при наличии payload причина `source_unavailable:playerStats:stored_source_omission` по **1** наблюдению оставляет результат `source_unavailable`. Источник примеров: `sample_rows` в `evidence-marker-summary.json`.

### Секции, применимость и unknown

Источник таблицы: `Six section comparisons` в `evidence-report-build.md`. Единица — scoped observation секции выбранного current payload в marker или historical scopes, а не отдельный marker и не вся required-когорта.

| Секция | Категория issue | Reason | Наблюдения |
| --- | --- | --- | ---: |
| lineup | confirmed_by_content | raw_typed_mask_agree | 266138 |
| lineup | source_unavailable | stored_source_omission | 271888 |
| matchFacts | confirmed_by_content | raw_typed_mask_agree | 538026 |
| momentum | confirmed_by_content | raw_typed_mask_agree | 101808 |
| momentum | unknown | malformed_or_unresolved_json | 436210 |
| momentum | writer_failed | raw_present_typed_absent | 8 |
| playerStats | confirmed_by_content | raw_typed_mask_agree | 152207 |
| playerStats | source_unavailable | stored_source_omission | 385819 |
| shotmap | confirmed_by_content | raw_typed_mask_agree | 538026 |
| stats | confirmed_by_content | raw_typed_mask_agree | 198311 |
| stats | source_unavailable | stored_source_omission | 339715 |

`raw_typed_mask_agree` фиксирует согласование сохранённого raw, typed-представления и mask по процедуре; само имя причины не обещает непустоту необязательной секции. `stored_source_omission` описывает отсутствие секции в сохранённом наблюдении источника. Это не вывод о постоянной недоступности секции для турнира и не результат live-пробы.

Ошибку загрузки (`fetch_failed`) следует отличать от доказанного source gap; ошибку парсера (`parse_failed`) — от непонятного или повреждённого evidence. Для momentum **436210** `malformed_or_unresolved_json` остаются **unknown**, без объявления ошибки парсера доказанной и без зачёта сбора. **8** `raw_present_typed_absent` выделены как `writer_failed`, поскольку raw присутствует, typed отсутствует. Эти категории нельзя объединять в «источник не отдаёт поле».

Полные сравнения всех шести секций: [section-evidence.csv](/root/data-platform-football/.local/tasks/2256-completion-audit/run-pass-c/passes/pass1/section-evidence.csv), SHA256 `6219196feb7c67e69c0ccbbdb6360f05d96d71e96d83b031b6381124f010b634`. `section-mismatches.csv` — точное подмножество non-confirmed строк, а не замена полного evidence.

Build и validation отдельно фиксируют **6898 unknown marker targets**, **547 unknown scoped ID**, **436210 unknown section observations**, **1789 unknown calendar scopes**. Это разные уровни, их нельзя складывать или трактовать как число матчей. Для ID известна причина `insufficient_absence_evidence`, для перечисленных section unknown — `momentum:malformed_or_unresolved_json`; полного распределения первопричин marker/calendar unknown во встроенной сводке нет. Оно остаётся в `reason_counts` и построчных результатах. Проверка `unknown_excluded_from_confirmed` имеет значение `true`.

## Поздние попытки и ограниченная сверка C1

По build у **3** current rows имеется более поздняя неуспешная попытка. Поздняя ошибка не удаляет last-good content. Это проверка сохранённой истории попыток и current identity, а не доказательство отсутствия любых поздних исправлений на live-источнике.

Источник таблицы: `Late failures and C1 (limited cohort)` в `evidence-report-build.md`. Точные наборы, наблюдения, deadline-day cohorts, unconfirmed/no-evidence IDs, обе snapshot identities и детерминированные hashes сохранены в `c1-reconciliation.json`.

| Метрика ограниченной C1-когорты | Значение |
| --- | ---: |
| Cached kind=input rows | 28053 |
| Resolved input rows | 28053 |
| Distinct scoped input IDs | 28053 |
| Checkpoint/control rows | 38 |
| Unresolved rows | 0 |
| Ever-ingested no-payload | 22211 |
| Current no-payload среди сопоставимых C1 IDs | 22286 |
| Current added since C1 absence observation | 0 |
| Ever-ingested but not current | 75 |
| Outside audit calendar | 0 |
| first_ft known finished | 5830 |
| first_ft finished без ever-ingested payload | 3 |
| first_ft с выполненным source-unavailable threshold | 0 |

C1 не покрывает всю историю и не заменяет основной знаменатель. Разные snapshots не обязаны давать равенство ever-ingested и current. Source-unavailable threshold требует отсутствия успеха и либо трёх запусков, либо двух запусков с разносом **7200 секунд**. Простое отсутствие current payload не удовлетворяет этому правилу.

## Исторические пары #1294 — отдельно от текущего знаменателя

Baseline **2026-10-21** сообщал **5305** finished без деталей всего, из них **1762** в проверяемых **18** парах. Эти исторические суммы не переносятся в текущую когорту. Ниже стоят текущие результаты каждой пары, полученные новой сверкой на frozen snapshots, а не подстановка old_gap. Источник baseline: build; источник каждой строки и причины: `evidence-historical-1294-results.csv`.

Во всех строках CSV `calendar_state=current`. Для Division Profesional причина W означает `writer_failed:expected_current_payload_missing`; S — `source_unavailable:repeated_source_data_not_found`. U означает worst-of `unknown` из секций momentum при отдельно установленной причине отсутствия payload S. Число отсутствующих payload и первичный итог scope различаются по смыслу.

| Competition ID / турнир | Сезон | Old gap | Current finished | С payload | Без payload | Actionable | Итог / причина gap |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 199 / Division Profesional | 2020 - Apertura | 132 | 132 | 0 | 132 | 132 | writer_failed / W |
| 199 / Division Profesional | 2020 - Clausura | 73 | 73 | 0 | 73 | 73 | writer_failed / W |
| 199 / Division Profesional | 2021 - Apertura | 90 | 90 | 0 | 90 | 90 | writer_failed / W |
| 199 / Division Profesional | 2021 - Clausura | 90 | 90 | 0 | 90 | 90 | writer_failed / W |
| 199 / Division Profesional | 2022 - Apertura | 132 | 132 | 0 | 132 | 132 | writer_failed / W |
| 199 / Division Profesional | 2022 - Clausura | 132 | 132 | 0 | 132 | 132 | writer_failed / W |
| 199 / Division Profesional | 2023 - Apertura | 132 | 132 | 0 | 132 | 132 | writer_failed / W |
| 199 / Division Profesional | 2023 - Clausura | 132 | 132 | 0 | 132 | 132 | writer_failed / W |
| 199 / Division Profesional | 2024 - Apertura | 132 | 132 | 0 | 132 | 132 | writer_failed / W |
| 199 / Division Profesional | 2024 - Clausura | 132 | 132 | 0 | 132 | 132 | writer_failed / W |
| 199 / Division Profesional | 2025 - Apertura | 132 | 132 | 0 | 132 | 132 | writer_failed / W |
| 199 / Division Profesional | 2025 - Clausura | 132 | 132 | 0 | 132 | 132 | writer_failed / W |
| 199 / Division Profesional | 2026 - Apertura | 132 | 132 | 0 | 132 | 132 | writer_failed / W |
| 199 / Division Profesional | 2026 - Clausura | 90 | 114 | 24 | 90 | 90 | writer_failed / W |
| 490 / Super Cup | 2024/2025 | 1 | 1 | 0 | 1 | 1 | source_unavailable / S |
| 10626 / FA Cup Qualification | 2025/2026 | 45 | 316 | 271 | 45 | 45 | unknown / S + U |
| 10626 / FA Cup Qualification | 2026/2027 | 51 | 326 | 275 | 51 | 51 | unknown / S + U |
| 11840 / Throne Cup | 2024/2025 | 2 | 16 | 14 | 2 | 2 | unknown / S + U |

У `199 / 2026 - Clausura` **24** payload согласованы по всем секциям, но **90** ожидаемых отсутствуют в current. У `10626 / 2025/2026` **271** finished с payload в буквальном срезе, тогда как completion reasons содержат **270** `current_payload_present` и **1** `cancelled`; это различие применимости, а не ошибка арифметики. Его **270** momentum observations — unknown. У `10626 / 2026/2027` momentum имеет **43** согласования и **232** unknown; у `11840 / 2024/2025` — **14** unknown. Для этих scopes подтверждённые причины отсутствия payload не делают все имеющиеся секции подтверждёнными.

Построчные хеши несовпадающих ID позволяют проверить результат каждой исторической пары. Источник таблицы: поле `current_mismatch_id_set_hash` в `evidence-historical-1294-results.csv`.

| Competition ID / сезон | SHA256 набора несовпадений |
| --- | --- |
| 199 / 2020 - Apertura | `f2725962e4f51e52c86e77d2b7f074fea8fe38a25ed000597be87d460e56d606` |
| 199 / 2020 - Clausura | `2d592afa53d9665068fc403ad9b7df0c732f61209097743b82c030e3237307a7` |
| 199 / 2021 - Apertura | `587e0e9d2ed808e9be322881dd5ec7d4472591e808996f1e00d3774fb4553d2b` |
| 199 / 2021 - Clausura | `c7ac6970fc9015c6e90dd4f7f915e6bdf1e3158e9cceb4f253cc36277b8aaa70` |
| 199 / 2022 - Apertura | `de09133b0bc56b26d552d78ac13ff47ce6e97d3b5871535294cdf098db24b6d1` |
| 199 / 2022 - Clausura | `1e09a3dc5809bd6637caba4362733af6efbb0f0aedfe66504bcb790182db5ce1` |
| 199 / 2023 - Apertura | `db7af645e43ecf96cfa3d58c500ee654f240ef47bbffb4a5a47b5db1805114f4` |
| 199 / 2023 - Clausura | `7466e303afc7c7c771dae767c17dd43a27526bff49848c9aca8e8a28d7f2d6b5` |
| 199 / 2024 - Apertura | `111d505ee836332abf7939f47711d81a15fe84e3e2fb351cce9746d6aef32ef9` |
| 199 / 2024 - Clausura | `1c1b0d77dea8fa224c8e18b748a27476561f3f4cace76810048471eddcc43b01` |
| 199 / 2025 - Apertura | `be5665b70f8545f68643d199f264f7720e3fb26ad2df3f8cbfed47288af0d94e` |
| 199 / 2025 - Clausura | `05e94f1473d52a58f0e7f442598f203bd53ca22519c9e8ed88c689e1543f013c` |
| 199 / 2026 - Apertura | `862981dd225c9e7c6e4b5eec9546f32fe394fc7ce076270cd92b0625b4d1e917` |
| 199 / 2026 - Clausura | `b14fe329608ba4bc9bcfb5bba5d2a8f7324cdcb68000acd0cda36c96223a1cfc` |
| 490 / 2024/2025 | `0fc309270c07e84b67287a1c3d48ac4fc5a006792536c1f96db9ce7e74f58448` |
| 10626 / 2025/2026 | `41a12682724f439ef6ee6c46ec63745b8935ac428e34eae6835755e65322ad4d` |
| 10626 / 2026/2027 | `829ec3e11efff44002178896bc05efd7499ca46645479d2c1897ac5366b21f36` |
| 11840 / 2024/2025 | `7a13aea0ff2ea29861d0087aea241c4f3cb9a001e62d036f92dec585b2073160` |

Исторический CSV хранит старые parser/plan signatures отдельно от текущего результата. Входные байты baseline и их SHA256 сохранены как input evidence по build; значение этого SHA256 в переданных материалах не раскрыто. Не следует подменять его хешем текущего набора несовпадений.

## Запросы, воспроизводимость и бюджет

SQL-сводка [queries.sql](queries.sql) описывает группы запросов, snapshot-предикаты и ограничение объёма. Исполняемый SQL принадлежит сохранённой процедуре в `scripts/research/fotmob_completion_audit/`; встроенные материалы не содержат физических имён таблиц, всех колонок, chunk-параметров и текстов выполненных statements. Поэтому сводка не выдаёт реконструированные шаблоны за исходный исполняемый SQL.

Для повторения сохраняются frozen snapshots, captured_at, план scopes/chunks, входной baseline, оба прохода, query ledgers и результаты verify/build. Ведущий повторяет сохранённую процедуру на том же срезе и сравнивает наборы ID, их хеши, identities payload и marker, reasons и результаты replay. Отсутствующий запрос, timeout или незавершённый проход не может давать валидный результат. Один build без второго прохода не доказывает replay.

Источник следующей таблицы: `Validation, timing and limits` в `evidence-report-build.md`. Строки и байты на каждый SQL во встроенном build не приведены; cohort totals и число результатов локальной классификации не являются объёмом одного SQL-ответа.

| Метрика | Значение из build |
| --- | --- |
| Queries | 2121 |
| Максимальное statement wall time | 20.867489 секунд |
| Лимит statement | 25 секунд |
| HTTP read deadline | 30 секунд |
| Local process timeout | 35 секунд |
| Поле «Pass wall time» | 9668.73838 секунд |
| Лимит на pass (фактический, run-pass-c) | 10800 секунд |
| Coverage validation | PASS |
| Replay | PASS |

Все statements по приведённому максимуму укладываются в **25 секунд**; `all_queries_successful` и `statement_budget` в validation — `true`. Query IDs, SQL SHA256 и outcomes сохранены в query ledgers, но сами записи ledgers здесь не предоставлены, поэтому идентификаторы и индивидуальные тайминги не выдуманы.

`Pass wall time = 9668.73838` укладывается в фактический лимит прогона **10800 секунд** (см. «Разбор отмеченных противоречий — ведущий» ниже); текст «3600» в build README — устаревший шаблон рендера. Все statements ≤ 25 с; `pass_budget` и `timing_reconciles` — `true`.

Черновик SQL и постановка приводят ориентир markers **7803 строки, около 2.5 секунды**, а черновик — snapshots **4 строки, менее 2 секунд**. Количество markers подтверждено итоговой сводкой; эти индивидуальные тайминги отсутствуют в переданном финальном build и не представлены здесь как проверенные замеры. Объём чтения в байтах не предоставлен.

`evidence-validation.json`: `status=PASS`, ровно **40** именованных checks, все `true` (пересчитано ведущим; раннее «43» было ошибкой пересчёта — см. разбор ниже). Ключевые проверки: `all_latest_marker_targets_once`, `historical_18_once`, `historical_baseline_preserved`, `scope_counts_and_id_hashes`, `six_sections_every_current_payload`, `section_mismatches_exact_subset`, `unknown_excluded_from_confirmed`.

## Границы результата

Аудит не обращался к live FotMob, не воспроизводил S3, не использовал current views, не выполнял DDL/DML и не менял DAG или расписания. Снимки разных входов имеют разные времена; их идентичности сохранены явно. Проверка не подтверждает изменения после этих снимков, универсальную доступность необязательных секций или полноту C1 за всю историю.

Coverage PASS означает покрытое и сохранённое evidence, а не универсальное подтверждение содержимого. Unknown остаются незачтёнными. `scope_completion` и `status=success` сами по себе не доказывают полноту. Выявленные writer/section gaps требуют отдельного разбора; этот отчёт не исправляет production, не объявляет зелёный день и не закрывает issue.

## Следующий шаг

Выполнено ведущим: frozen run сохранён, `marker-verdicts.csv` опубликован с SHA256, противоречия wall time/лимита и счётчика checks разрешены (раздел ниже). Остаётся: разобрать найденные writer/section gaps (199 Division Profesional — 1806 исторических без payload, momentum unknown 436210) отдельными задачами в разрешённом владельцем объёме.

Разбор `expected_current_payload_missing`, `raw_present_typed_absent` и `malformed_or_unresolved_json` оформляется отдельно в разрешённом владельцем объёме. Для #2256 приёмка — одна воспроизводимая read-only проверка без шага на production; документирование её результатов не запускает новую серию наблюдений.

## Разбор отмеченных противоречий — ведущий (2026-11-17)

1. **Pass wall time 9668.73838 с против «лимита 3600 с».** Доказательство повышения лимита до 10800 с до прогона run-pass-c — сам код процедуры, которой выполнялся прогон: `scripts/research/audit_fotmob_completion.py` (SHA256 `bf7ef3d40027c6625fe76f5143020f15f755648e64a09e2d82031f9e3787522c`), строки: `LIMITS = {... "pass_seconds": 10800, ...}` и `Queries(..., budget=LIMITS["pass_seconds"])`. Косвенное доказательство из прогона: pass1 завершился `complete=true` с `pass_wall_seconds=9668.73838` и `pass_budget=true` — при лимите 3600 это невозможно (см. `run-pass-c/passes/pass1/{status,timing}.json`). Текст «3600» в build README — устаревший захардкоженный шаблон рендера. Правка лимита — раскрытое решение ведущего (журнал задачи), бюджет issue задаётся процедурой, каждая statement ≤ 25 с подтверждена независимо.
2. **Счётчик проверок.** В `validation.json` ровно **40** именованных checks, все `true`, `status=PASS`. Заявленные ранее «43» — ошибка пересчёта ведущего; исправлено.
3. **Индивидуальные тайминги запросов.** Полные query ledgers сохранены и доступны для проверки: `run-pass-c/passes/pass1/query-ledger.csv` (SHA256 `61ef8243111b29400e483a24d6dbb1bc614710cc8025fd4d784a6503a9fb5c3e`, 2121 строка) и `run-pass-c/passes/pass2/query-ledger.csv` (SHA256 `7632c7d73e4653e30578ac7d2355d8d8aba15edc78c5d9ad57daae41968582dd`); каждая строка содержит query_id, SQL SHA256, rows, wall_seconds, status, error_class. `max_statement_seconds=20.867489` — максимум по этим ledger-строкам.
4. **marker-verdicts.csv (реализация критерия 2 «для каждого v2-маркера зафиксирован verdict»)**: 7803 строк (одна на маркер), SHA256 `7dc097796a85cd231969f55b2d2a2f1173607383d2869c5d452913dae302eeed`, файл `docs/research/fotmob-completion-audit-2026-11/marker-verdicts.csv`. Запись публикации с исходным SHA256 и описанием трансформации `confirmed_content → confirmed_by_content`: `marker-verdicts-publication.json` (SHA256 `a9166e8b4b6cff7bce933d2628acda517cab5dbaafc081bf486dcadae17b152f`); исходный `marker-results.csv` не изменялся.
