# Transfermarkt: локальный intake D3

`dags/scripts/prepare_transfermarkt_offline.py` проверяет уже сохранённые данные.
Он не выполняет HTTP/SQL, не меняет runtime, лимиты, ledger, first_seen, t0,
критерии, holds или состояние приёмки. Разрешения на доставку и запуск остаются
отдельными. Проверка fixture не доказывает настоящие наблюдения источника.

## Существующий RawResponseStore

```bash
PYTHONDONTWRITEBYTECODE=1 python dags/scripts/prepare_transfermarkt_offline.py export-raw \
  --raw-store /absolute/path/to/saved/raw \
  --output /absolute/path/to/new/local/export
```

Адаптер читает `captures/sha256/*/*.json` и соответствующие gzip blobs через
`RawResponseStore.load_capture()`. Для сохранённых container URI используется
исходный `uri_prefix` и **локальная** Arrow filesystem. Адаптер проверяет capture
identity, blob size/hash и original clock. Он копирует исходный manifest без
изменений и сохраняет декодированные bytes в отдельный каталог. `captures.json`
содержит относительные `body_path` / `receipt_path`. Повторная запись в тот же
каталог запрещена. URI из manifest не открывается по сети.

Failed captures также сохраняются. Status 405 не становится успехом. Intake не
повторяет запрос и не сбрасывает tries2/2. `export.json` сообщает
`exported_not_qualified`, исходные capture IDs, status/attempt/clock и недостающие
предпосылки. Существующий experiment ledger остаётся авторитетным для расходов
и exhaustion. Адаптер не выводит из одного capture разрешение на новый запрос.

## Полный registry pin и фиксированная выборка

```bash
PYTHONDONTWRITEBYTECODE=1 python dags/scripts/prepare_transfermarkt_offline.py pin \
  --registry registry-query-rows.json --inventory registry-inventory.json \
  --denominator denominator.tsv --cohort cohort.json \
  --mappings source-mapping-requests.json --capture-index export/captures.json \
  --output pinned --evidence-kind exported
```

`--registry` — полный экспорт строк `build_promoted_registry_query()` с
`registry_snapshot_id=<fixed snapshot>, current_only=False`. UNKNOWN, inactive и
noncurrent строки сохраняются. Нужен отдельно выгруженный inventory того же
snapshot с полями:

```json
{
  "registry_snapshot_id": "<snapshot>",
  "status": "promoted",
  "promotion_revision": 123,
  "full_registry": true,
  "query_id": "<original query ID>",
  "query_sha256": "<SHA256 exact pinned full query UTF-8>",
  "exported_at": "<original ISO8601 timestamp with zone>",
  "row_count": 1234,
  "all_keys": [["<competition_id>", "<edition_id>"]],
  "current_keys": [["<current competition_id>", "<edition_id>"]],
  "registry_sha256": "<SHA256 exact registry file bytes>",
  "snapshot_fingerprint": "<SHA256 canonical full row array>"
}
```

Оба key lists сортируются по competition/edition. Fingerprint использует
`json.dumps(rows, sort_keys=True, separators=(',', ':'), ensure_ascii=False)` в
UTF-8. Все keys, полное число строк, current subset, snapshot/version, query и
оба hash должны совпасть. Duplicate keys запрещены. Самостоятельно составленный
inventory не доказывает полноту production. Offline невозможно подтвердить
происхождение выгрузки. `source_authenticity=not_authenticated_offline` сохраняется
даже при структурном совпадении. Получение настоящего полного export и сверка с
production остаются отдельной предпосылкой.

`--cohort` имеет формат сохранённого candidate manifest: `created_at`,
`player_ids`, `clubs` с `club_id`, `scope`, `groups`, `player_ids`. Выборка содержит
не менее 1000 уникальных игроков, 20 scopes и все четыре группы. Каждый ID
закреплён за одним club/scope. Список целиком входит в pin и не сокращается при
ошибке. Это позволяет использовать подготовленный historical candidate как
вход, но не объявляет его current без source proofs.

`--mappings` — список `{"competition_id":"GB1","capture_id":"<original ID>"}`.
Исходный raw должен быть unforced **competition** profile с selected edition и
таблицей участников. Существующий discovery parser читает selector/title и
season format. Club selected season не доказывает current edition соревнования.
Несовпадения и отсутствующие proofs остаются blockers. Полный daily denominator
сохраняет missing/unknown соревнования. Source proofs нужны для каждого scope
фиксированной qualification выборки; отсутствие proof вне выборки не становится
новым глобальным gate и не расширяет bootstrap budget.

`pinned/registry.json` и `pinned/denominator.tsv` совместимы с существующим
`dags/scripts/transfermarkt_daily_report.py --registry-json ... --denominator ...`.
Pin сохраняет denominator hash, definition hash, current edition fingerprints,
original inventory и raw receipts. Это подготовка SELECT, а не его выполнение.

## Два наблюдения

```bash
PYTHONDONTWRITEBYTECODE=1 python dags/scripts/prepare_transfermarkt_offline.py qualify \
  --pin-directory pinned --day1 day1-report.json --day2 day2-report.json \
  --capture-index parity-export/captures.json --output qualification \
  --evidence-kind exported
```

Оба отчёта используют существующий `bench_transfermarkt_signals` parity contract.
Intake требует точную закреплённую выборку, все source current mappings и
исходные day1 club/player assignments. На day2 игрок может перейти в другой
клуб: новый full roster и tmapi должны подтвердить переход, а клуб должен
принадлежать закреплённому eligible current scope по source mapping. Исходные
IDs, scopes, groups и day1 assignments сохраняются. Каждый scope по-прежнему
должен иметь реально наблюдаемых игроков; исчезнувший scope нельзя исключить
из выборки. Четыре группы проверяются по pinned registry и tier из
denominator. Непредставленные scopes нельзя записать в cohort ради количества.
Original registry export и source mapping должны предшествовать day1 и быть не
старше 24 часов к его началу. Для exported input сам pin создаётся **до** day1;
ретроспективный выбор cohort не проходит.

Все tmapi/full plus1/оба ceapi body hashes, capture IDs и clocks проверяются по
сохранённым raw. Существующий `parity_report()` заново строит comparisons из
bytes. Значения из JSON отчёта должны точно совпасть. Затем существующий
`build_qualification()` проверяет missing/unknown, >=1000 IDs, >=20 scopes,
consecutive Moscow days и настоящее согласованное same-day изменение поля.
Неполный отчёт, failed405, unknown ID, подменённый clock, различие bytes или
показания без изменения не дают успешной qualification.

В `result.json` всегда `runtime_admission=false`, `launch_authority=false`,
`D2_decision_required=true`, `genuine_runtime_evidence_required=true`. Offline
успех означает проверку локального export contract. Он не подтверждает genuine
production evidence, физический Bronze readback, неизменность first_seen при
replay, DQ, 3 полных суток #1395 или разрешение активации. Существующий builder
работает только во временном приватном каталоге. Его runtime-compatible
`parity-evidence.json` удаляется после вычисления, включая исключения. В итоговом
каталоге остаются исходные parity reports и результат offline intake. Existing
runtime validator отвергает каждый из этих JSON: ни fixture, ни ярлык exported
не создают допускаемый runtime artifact.

`--evidence-kind fixture` даёт только `fixture_validated`. Для воспроизводимого
исторического fixture допускается проверка original simulated clocks после
создания локального pin. Этот режим никогда не называется genuine. Изменить
метку fixture на exported в qualification нельзя. Ярлык exported также не
служит доказательством подлинности.

Focused offline tests:

```bash
env -u PYTEST_ADDOPTS -u PYTEST_PLUGINS \
  PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 \
  /root/.venvs/dpf-test/bin/python -m pytest -q -p no:cacheprovider \
  tests/unit/scrapers/test_transfermarkt_offline_qualification.py \
  tests/unit/scrapers/test_transfermarkt_signal_qualification.py \
  tests/unit/utils/test_transfermarkt_daily_measure.py
```

Полный unit suite и отдельное review обязательны для последующего PR. Этот
документ не заменяет действующие условия #1393/#1395/#1652/#1658.
