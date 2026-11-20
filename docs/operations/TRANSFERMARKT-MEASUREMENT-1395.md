# Измеритель суток Transfermarkt (#1395)

`dags.utils.transfermarkt_daily_measure` читает существующие checks/signals
#1393 одним параметризованным SELECT. Второго журнала проверок нет. CLI
`dags/scripts/transfermarkt_daily_report.py` генерирует SELECT с bindings либо
читает уже выгруженный результат. С флагом `--execute` CLI читает Trino.
Датированные контрольные снимки сайта CLI принимает из файлов.

## Неизменяемое первое наблюдение

`scrapers.transfermarkt.first_seen.prepare_frames` добавляет `first_seen_at`
в новые физические строки `squad_memberships`, `market_value_points`,
`transfer_events` и `coach_stints`. Ключи совпадают с существующими native
контрактами, включая NULL-safe дату окончания тренерского срока. Batch,
источник scope и дата последующей перекачки не входят в ключ глобальной карьеры.

Первый оригинальный `fetched_at` закрепляется в
`iceberg.ops.transfermarkt_first_seen_v1` под существующим writer lock, затем
подтверждается чтением. Это происходит до сохранения карьерного write-intent
и до изменения Bronze. Schema evolution nullable столбца выполняет writer
при записи. Реестр не удаляется при typed empty или замене partition:
возвращённый факт получает прежнее время. Регистрация или readback с ошибкой
не разрешают продолжать запись. First seen не доказывает момент публикации
сайтом и не заменяет настоящий commit clock.

Старая когорта остаётся неизвестной. Её последнее fetched_at не выдаётся за
историческое первое наблюдение. Старые intents, retained snapshots, raw,
batch и hashes воспроизводятся в исходной проекции. Если старый intent пишет
факт с уже зарегистрированным временем, outgoing writer сохраняет это время,
не меняя исходного journal. Новые current receipts сохраняют список физических
столбцов; старые receipts продолжают проверяться по прежней проекции.

Current/history standing policies получают только разрешение на этот ops-реестр.
Сохраняются версии 4/2, сроки, caps и concurrency. Совместимость старых pinned
hashes ограничена точными разрешёнными наборами добавленных ops-таблиц;
изменение caps или остальных прав не принимается как эта миграция. History
остаётся выключенной. Legacy и Silver/Gold схемы не меняются.

## Знаменатель и охват

`daily_denominator` получает актуальные строки promoted registry через
существующий scope query. По умолчанию live core по-прежнему читается из
`configs/transfermarkt/denominator.tsv` (`TRANSFERMARKT_DENOMINATOR_SOURCE=tsv`),
а версией знаменателя является SHA256 файла. Новый адаптер
`dags/utils/transfermarkt_denominator_registry.py` поддерживает opt-in режим
`registry`: он строит тот же `Denominator` из одного опубликованного snapshot,
а `universe_version` равен `registry_snapshot_id`. Это не активный production
режим и не fallback: при ошибке registry режим падает, `tsv` остаётся откатом.

Registry `eligible` не равен актуальному core: адаптер повторяет tier/live
фильтры канона по `section_label` и текущему изданию. Юношеские/резервные —
отдельные классы вне приёмки, женские исключены. `title_only` — независимый
флаг, а quarantine — отдельный список ID/reason без выдуманных строк. Unknown
live/tier/geography не допускается молча. Известные не-live строки — `archive`
с сохранением subtype. 1824 competitions / 25754 editions — факты публикации,
не знаменатель покрытия.

`denominator_hash` совпадает с hash текущего runner. Отдельно сохраняются версия
знаменателя, версия измерителя и registry snapshot для каждого издания.
Количество не зашито: исторический TSV core содержит 547 соревнований, новый core
вычисляется из snapshot. Отсутствующие, заблокированные и неизвестные текущие
издания остаются в знаменателе. Tail и архив в процент не входят. Несколько
текущих изданий соревнования требуют успеха каждого; единица знаменателя
остаётся соревнованием.

При сравнении обе стороны используют один экспорт текущих изданий, точный
манифест discovery, одни отчётные сутки МСК и одну отсечку `as_of`.
Измеритель закрепляет физические Iceberg-снимки журналов проверок и сигналов.
Сохранённый `--journal-snapshots-json` воспроизводит эти входы для повторного
измерения и отката. В режиме `--registry-json` CLI выдаёт SQL и параметры;
режим `--execute` возвращает результаты SELECT. При исчезновении legacy core
или недостоверных входах сравнение завершается с ненулевым кодом.
[Команды, критерии сравнения и проверка трёх суток](../../configs/transfermarkt/README.md#offline-migration-check-and-rollback-2331).

Сутки — `[00:00, 00:00 следующих суток)` МСК. SQL timestamp(6) хранит UTC.
Успех требует complete listing, players и injury, полный expected/observed
набор и непустые raw references. Старые записи требуют matching полного hash
и текущего promoted snapshot. Новые записи также допускают прошлый promoted
snapshot при совпадении edition_fingerprint и denominator_definition_hash:
смысл издания и состав live core должны остаться прежними. Fingerprint включает
классификацию, активность, current, канонический сезон, участников и версии
parser/schema. Обновление snapshot/времени discovery его не меняет; новое издание
или изменение участников его меняет. Unknown старые fingerprints не обходятся.
Исходные snapshot/hash сохраняются в checks; SQL bindings содержат актуальные
издания, snapshots, stable fingerprints и версию файла знаменателя.
Для national_team injury требует явного подтверждения not_applicable с правилом
и snapshot. Ошибки не продлевают время успешной проверки. Runner сохраняет
самое раннее исходное время raw набора; cache и восстановление его не обновляют.

## Свежесть изменений

`measurement_kind` — добавленная колонка signals и поле superseded evidence:
`cold`, `baseline`, `change`, `unknown`. Старые SQL/JSON записи читаются как
unknown. Изменение не меняет parser/schema revisions, сигнатуры, batch identities
или статус обработки. Неизменённый повтор сохраняет исходную категорию.
Миграция колонки идемпотентная. Superseded events сохраняют прежние ошибки,
исходные detection/commit clocks и raw lineage.

SQL использует изменения с first_detected_at в заданные сутки, включая
superseded failures. Успех — applied с Bronze manifest и commit в интервале
`[first_detected_at, first_detected_at + 24ч]`. Injury raw_durable не является
записью Bronze и в этот процент не входит. Cold, baseline и неизвестные события
показываются отдельно. В процент входят только события, чьи 24 часа уже прошли
на as_of. Незрелые изменения и pending показаны явно. Нулевой знаменатель даёт
NULL («не доказано»). Суточная когорта окончательно созревает day_end + 24ч.

first_detected_at измеряет задержку после обнаружения нашим сборщиком.
Неизменяемый Bronze first_seen_at не является commit time или датой публикации
на сайте. Эти часы не заменяют контроль свежих страниц.

## Воспроизведение и общая строка

```bash
python dags/scripts/transfermarkt_daily_report.py \
  --day 2026-10-08 --as-of 2026-10-10T00:00:00Z \
  --registry-json /evidence/promoted-scope-rows.json
```

Ответ содержит `sql`, `params`, hash/version и blockers. Передайте SQL и bindings
существующему read-only Trino client. Сохраните один объект результата и материалы
в каталоге задачи. JSON export timestamp без timezone трактуется как UTC согласно
ops-контракту.

```bash
python dags/scripts/transfermarkt_daily_report.py \
  --day 2026-10-08 --as-of 2026-10-10T00:00:00Z \
  --report-json /evidence/daily-select-result.json \
  --samples-json /evidence/samples.json --dq-json /evidence/daily-dq.json
```

CLI проверяет версию, границы суток и `as_of` результата. Для DQ он читает
отчёт за тот же `day_msk` с подходящей версией и `cutoff_at <= as_of`.
Вместо `--dq-json` можно указать каталог готовых отчётов в `--dq-state-root`.
Незавершённый RED виден с пометкой о неполной проверке. Отсутствующий или
устаревший отчёт даёт неизвестный статус.

### Подключение к утренней сводке

Кандидат для хостовой функции `transfermarkt_measure_dq()` находится в
[`transfermarkt-morning-report.patch`](../../dags/scripts/transfermarkt-morning-report.patch).
Он вызывает [адаптер](../../dags/scripts/transfermarkt_morning_report.py) в окружении
планировщика. По умолчанию адаптер берёт предыдущие календарные сутки МСК и
фиксирует время расчёта в UTC. Из завершённых к этому моменту плановых порций
выбирается последняя с `finished_at` в отчётных сутках. Это определяет снимок
для суточного SELECT даже при смене реестра внутри дня.

В строке видны точный `as_of`, источник знаменателя, его версия, хеш состава
и снимок реестра. В JSON также сохраняется идентификатор порции. При историческом
повторе сохраняйте исходное время изменения файлов порций. Адаптер читает
только квитанции, доступные к `as_of`. При несовпадении знаменателя с SELECT строка
показывает недоступность измерения. Блокированные соревнования входят в знаменатель.
Переход планировщика с TSV на реестр ведётся в #2331.

Пример повтора для завершённых суток 18 ноября.

```bash
python -B dags/scripts/transfermarkt_morning_report.py \
  --day 2026-11-18 --as-of 2026-11-19T04:00:00Z
```

Для независимой сверки прочитайте promoted-строки через
`build_promoted_registry_query(registry_snapshot_id=…, current_only=True)`
и сохраните их в JSON. Передайте их штатному CLI с `--registry-json` и теми же
`--day`, `--as-of`, `--denominator-source` и, для TSV, `--denominator`.
Выполните возвращённые `sql` и `params` через read-only Trino-клиент.
Сравните все поля строки SQL, включая хеш и версию знаменателя. Для сравнения
текста передайте ручную строку штатному CLI через `--report-json` с тем же
дневным DQ и, при наличии, тем же `--samples-json`.

До применения патча требуется отдельное разрешение владельца. Проверьте
`git apply --check` на свежей копии хостового файла и вызовите только
`transfermarkt_measure_dq()` из этой копии. После разрешённой установки
сохраните настоящую строку и ручной результат за каждый из трёх московских
дней. Сверка до установки служит доказательством разработки.

### Определение приёмки

[Предлагаемые критерии #1395](transfermarkt-1395-criteria.json) привязаны к
SHA-256 полного тела issue. Они дополняют действующую запись с тем же ID.
Перед изменением сверьте полный текст GitHub, свежую ревизию журнала и оба
удержания. При подготовке входа для `revise` сохраните метаданные прежней
записи, в том числе `close_hold=true` и `auto_close_issue=false`.
В установленной версии журнала `revise` открывает новую серию и сбрасывает
`impact_hold`. Пока это удержание действует, разрешена репетиция на копии.
Для каждого зачтённого дня нужны строка боевой сводки, совпавший ручной SQL,
полный ежедневный DQ и обе проверки `first_seen_at` по четырём таблицам.

## Контрольная выборка без новых запросов

`samples.json` — массив записей со scope_id, player_id, club_id, field
(`market_value_eur` или `contract_until`) для kind=roster_field по умолчанию,
previous/current с относительными
`body_path` и `receipt_path`. Импортёр проверяет успешные raw receipts, SHA256
исходных тел и их URL/scope/time. Для roster_field оба тела — полная /plus/1 страница одного
клуба с Contract header; игрок присутствует на обеих страницах. Реальный parser
подтверждает изменение известного поля. Расстояние между наблюдениями — ≤24ч;
current находится в заданных сутках. Старые страницы, дубли и пути за пределы
каталога доказательств отклоняются.

Также поддерживаются kind=roster (новый игрок в составе), transfer
(`/ceapi/transferHistory/list/{player_id}`), valuation
(`/ceapi/marketValueDevelopment/graph/{player_id}`), coach
(`/mitarbeiterhistorie/verein/{club_id}/plus/1`). Используются существующие
endpoint-specific parsers. Для последних трёх укажите row_key: соответственно
transfer_id; mv_date; coach_id+appointed_date+role. Новый/изменившийся факт
сверяется с native transfer_events, market_value_points или coach_stints;
новое членство — с squad_memberships. Coach import не включает detector #1396.
Unsupported kind или неоднозначный natural key остаются unproven, а не
превращаются в синтетическую строку. Upcoming/unknown transfer не подтверждает
завершённый переход. Повреждённые raw/lineage/readback отклоняются.

Для записанного изменения добавьте `bronze_receipt_path`, `readback_path` и
`output_key`. Receipt — реальный business receipt текущего writer с outputs,
успешными native/dual manifests, checksum и committed_at. Readback JSON содержит
`table`, `batch_id`, `columns`, `rows` точного физического набора. Импортёр
пересчитывает physical_hash способом writer, сверяет manifest/table/batch/count,
затем проверяет изменённое поле и URL/body hash/fetched_at конкретной строки.
Один флаг verified без этих материалов не подтверждает приёмку. Без receipt
изменение остаётся pending и после созревания входит как неуспешное.

`site_first_seen_at` выборки — время первого raw с изменённым полем в проверенной
паре, а не неизвестная дата публикации. Предыдущее наблюдение задаёт интервал
возможного появления изменения. Для доказанного ≤24ч от появления commit должен
быть не позднее previous_observed_at +24ч: это консервативная нижняя оценка доли.
Commit в оставшемся интервале до site_first_seen_at +24ч показывается отдельно
как appearance_interval_unproven и не создаёт ложного успеха. Отсутствие выборки даёт «не доказано», не 100%.
Fixtures и offline-import не подтверждают live sampling или runtime-приёмку.
