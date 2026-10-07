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
их ради зелёного результата. Общий [CI unit-suite](.github/workflows/ci.yml)
явно исключает шесть WhoScored host-seal файлов; это существующее разделение CI и
локального full suite, не разрешение копировать исключения в локальную команду.
Source workflows в [.github/workflows](.github/workflows/) проверяют более глубокие
контракты и настоящий DagBag в образе. Их профили и обязательность сохраняются.

Не используй голый `pytest`: [pyproject.toml](pyproject.toml) выбирает всё `tests`,
включая live-интеграции. Не все offline-тесты лежат в `tests/unit`: например,
unit-проверки в [test_bi_catalog_scripts.py](tests/integration/test_bi_catalog_scripts.py)
нужно выбирать дополнительно с `-m unit`, если затронут их код.
Старый `make test-fbref-offline` обращается к общему scheduler и блокируется
его wrapper; для локального прогона используй host test-venv и явные test paths
из [.github/workflows/fbref-ci.yml](.github/workflows/fbref-ci.yml), не обходи wrapper.

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
