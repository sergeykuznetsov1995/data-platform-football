# Проверки из отдельной рабочей копии

Все команды ниже выполняются из корня своей копии репозитория, не из смонтированного
production-дерева. Для smoke не нужны Docker, Airflow, `.env`, токены или доступ к
источникам. Установка пакетов требует package index или заранее подготовленного
локального wheelhouse; сами smoke-тесты работают offline.

## Быстрая проверка окружения

Профиль: Linux, Python **3.12** (проверен 3.12.3), pytest **9.0.3**. Создай отдельный
venv; не устанавливай и не обновляй пакеты в общем или production-окружении.
Ниже закреплены pytest и все его зависимости для Python 3.12/Linux:

```bash
python3.12 -m venv .venv-smoke
.venv-smoke/bin/python -m pip install --no-deps \
  pytest==9.0.3 iniconfig==2.3.0 packaging==26.2 pluggy==1.6.0 Pygments==2.20.0
.venv-smoke/bin/python -m pip check
env -u PYTEST_ADDOPTS -u PYTEST_PLUGINS \
  PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 \
  .venv-smoke/bin/python -m pytest -q -p no:cacheprovider \
  tests/unit/utils/test_sofascore_red_share.py
```

Ожидается **12 passed**, exit 0. Тест проверяет чистые функции и импорт без Airflow;
не делает HTTP/SQL-запросов и не запускает сервисы. Он подтверждает только этот
узкий offline-профиль, не корректность источника, full suite или production.
В уже подготовленном test-venv можно заменить путь к Python; сначала зафиксируй его
версию и `python -m pip check`. Наличие venv на конкретном сервере не является
предпосылкой клонирования проекта.

Автозагрузка внешних pytest-плагинов отключена, как в общем CI: плагины из соседних
пакетов (например, rerunfailures) могут открывать сокет ещё до запуска тестов.
Для теста, которому нужен плагин, подключай его явно через `-p` в соответствующем
профиле. Не ослабляй sandbox ради автозагрузки. `PYTEST_DISABLE_PLUGIN_AUTOLOAD`
не запрещает сеть тестируемому коду: offline-безопасность определяется выбранным тестом.

## Полный unit suite и обязательные CI

Для кода, SQL, DAG, зависимостей или конфигурации приложения полный локальный suite
обязателен перед PR. Между итерациями достаточно затронутых тестов. Для изменения
только документации — содержимое, ссылки и загрузка; инструкции дополнительно
проверяются в новой сессии. CI-гейты и защита ветки от этого не отключаются.

Полный профиль использует Python 3.12 и [requirements-ci.txt](requirements-ci.txt).
Airflow намеренно отсутствует: unit DAG-тесты используют stubs. Минимальный smoke-venv
не подходит для full suite. Пример отдельного полного test-venv:

```bash
python3.12 -m venv .venv-test
.venv-test/bin/python -m pip install -r requirements-ci.txt
.venv-test/bin/python -m pip check
env -u PYTEST_ADDOPTS -u PYTEST_PLUGINS \
  PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 \
  .venv-test/bin/python -m pytest -q -p no:cacheprovider tests/unit
```

Полный suite содержит проверки предпосылок рабочего хоста. На другом компьютере
они могут падать; сохраняй вывод и сравнивай с базой в таком же окружении. Не убирай
их ради зелёного результата. Процессные unit-тесты WhoScored используют явную
fixture с `/usr/bin/unshare`, версией util-linux 2.39.3 и двумя закреплёнными SHA256
сборок; остальные байты отвергаются. Fixture действует только в выбранных тестах,
production admission pins не меняет. Реальные PID namespaces, prctl, завершение
потомков и проверки metadata/seals сохраняются; нужен root Linux-хост с разрешением
на PID namespace. Проверки Python-зависимостей отдельно изолируют host-preflight;
негативные тесты helper подтверждают отказ до запуска worker.
Общий [CI unit-suite](.github/workflows/ci.yml)
явно исключает шесть WhoScored host-seal файлов; это существующее разделение CI и
локального full suite, не разрешение копировать исключения в локальную команду.
Source workflows в [.github/workflows](.github/workflows/) проверяют более глубокие
контракты и настоящий DagBag в образе. Их профили и обязательность сохраняются.

Не используй голый `pytest`: [pyproject.toml](pyproject.toml) выбирает всё `tests`,
включая live-интеграции. Не все offline-тесты лежат в `tests/unit`: например,
unit-проверки в [test_bi_catalog_scripts.py](tests/integration/test_bi_catalog_scripts.py)
выбираются явно в подготовленном test-venv:

```bash
env -u PYTEST_ADDOPTS -u PYTEST_PLUGINS \
  PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONDONTWRITEBYTECODE=1 \
  python -m pytest -q tests/integration/test_bi_catalog_scripts.py -m unit
```

Состав общего автоматического гейта указан в [.github/workflows/ci.yml](.github/workflows/ci.yml);
весь каталог integration для offline-проверки не запускай.

## Offline-профили источников и FBref Make

Шесть [закреплённых test-профилей](requirements/test/README.md) сохраняют существующие
Python 3.11/3.12 и прямые/транзитивные версии успешных source CI. Это отдельные
Linux x86_64 CPython minor-профили с wheel hashes; зависимости приложения и
Airflow release constraints от них не меняются. Подготовка FBref:

```bash
python3.11 -m venv /tmp/dpf-fbref-test
. /tmp/dpf-fbref-test/bin/activate
python -m pip install --disable-pip-version-check --require-hashes \
  -r requirements/test/fbref-unit-py311.lock
python -m pip check
make test-fbref-offline
```

Make использует `python3` активированного venv; `PYTHON=/path/to/test-venv/bin/python`
выбирает другой тестовый интерпретатор. Локальный runner и
[FBref CI](.github/workflows/fbref-ci.yml) выбирают одинаковые обычные
`tests/unit/**/*fbref*.py` и пять maintenance/proxy extras. Runner очищает
`PYTEST_ADDOPTS`/`PYTEST_PLUGINS`, выключает autoload и возвращает exit pytest;
отсутствие pytest или FBref-тестов — ошибка. Сам runner не устанавливает пакеты
и не вызывает Compose; защита общего scheduler сохраняется.

Offline означает отсутствие запросов к источникам или production. Некоторые
control/proxy unit-тесты используют временные TCP-заглушки на `127.0.0.1`.
Среда, запрещающая любые sockets, не сможет запустить эти fixtures; для отдельной
локальной проверки используй штатное разрешение на loopback, сохраняя отключённую
автозагрузку плагинов и защитный wrapper. Production для этого не нужен.
PostgreSQL/S3 semantics, Compose render и реальные Airflow imports остаются
отдельными CI-проверками с временными сервисами. Wheelhouse-подготовка и обновление
профилей описаны в их README; новый lock должен пройти чистую установку и source checks.

## Интеграции и SQL

Интеграции, live HTTP, S3/Trino и реальный Airflow выполняются только по регламенту
затронутого источника из [карты](docs/operations/README.md), с установленными
эксплуатационными условиями. Пропущенный тест не подтверждает импорт DAG; CLI DagBag
не доказывает, что production scheduler перечитал доставленный код.

При изменении Silver SQL дополнительно выполни статический аудит в test-venv:

```bash
.venv-test/bin/python scripts/audit_silver_charter.py --check
```

Сохраняй SHA, версии runtime, точные команды, exit code и число passed/failed/skipped.
Не называй прогон успешным при collection errors или неполном выводе.
