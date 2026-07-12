# Доступ юзеров к платформе

Дизайн и устройство: [design/analyst-access.md](design/analyst-access.md).
Домен: `sk-vpn-2026.uk`. **VPN не нужен никому** — все сервисы публичные,
за Keycloak SSO (историю VPN-схемы см. [HEADSCALE_MIGRATION.md](HEADSCALE_MIGRATION.md)).

## Два класса юзеров

| Класс (группа Keycloak) | Что получает |
|---|---|
| `viewers` — «простой» юзер | только Superset-дашборды |
| `analysts` — аналитик | всё из таблицы ниже |

| Сервис | Адрес | Кому | Права |
|---|---|---|---|
| Superset | `https://bi.sk-vpn-2026.uk` | все | дашборды (viewers) + SQL Lab (analysts) |
| JupyterHub | `https://jupyter.sk-vpn-2026.uk` | analysts | свой контейнер (1 ГБ), диск |
| Airflow | `https://airflow.sk-vpn-2026.uk` | analysts | просмотр DAG'ов (Viewer) |
| Trino | `https://trino.sk-vpn-2026.uk` | analysts | SQL read-only `silver`/`gold` |
| OpenMetadata | `https://meta.sk-vpn-2026.uk` | по запросу | каталог данных |

OpenMetadata — вход через тот же Keycloak (#866), но **по запросу**:
self-signup в каталоге выключен, юзера в OM заводит админ
(OM UI → Settings → Users → Add User, email = email юзера в Keycloak),
после этого работает обычный SSO-вход. Писать в данные нельзя:
любой не-машинный юзер получает от rules.json только SELECT на
`silver`/`gold` (catch-all; правки конфигов на нового юзера НЕ нужны).
Запросы людей идут в ресурсной группе `humans` (лимит 8 одновременно,
30% памяти) и не могут задушить пайплайны.

## Онбординг — ~1 минута, любой класс

1. Аккаунт одной командой (создаёт юзера в Keycloak с временным паролем
   и кладёт в группу):

   ```bash
   scripts/onboard_user.sh <username> <email>            # viewer (default)
   scripts/onboard_user.sh <username> <email> analysts   # аналитик
   ```

2. Отправь юзеру временный пароль из вывода скрипта + ссылки
   (viewers достаточно `https://bi.sk-vpn-2026.uk`).
3. Первый вход сам заведёт юзера в каждом сервисе с нужной ролью
   (viewers в JupyterHub/Airflow не пускаются).

Руками через админку Keycloak — тоже можно (см. «Админ-доступ»):
Users → Create user → Credentials (Temporary) → Groups → Join.

Админа платформы заводят так же (группа `platform-admins`), плюс строка
`platform-admins:` в `configs/trino/groups.txt` — это единственный случай,
когда groups.txt ещё нужен.

**Миграция с VPN-схемы (разово):** у старых аналитиков удалить
Tailscale-клиент; на macOS обязательно удалить файл-резолвер, иначе домен
перестанет открываться (резолвер указывает на мёртвый VPN-DNS):

```bash
sudo rm /etc/resolver/sk-vpn-2026.uk
sudo dscacheutil -flushcache; sudo killall -HUP mDNSResponder
```

В DataGrip/DBeaver перепроверить подключение Trino:
host `trino.sk-vpn-2026.uk`, порт 443.

## Оффбординг

1. Keycloak → Users → Disable.
2. Только для аналитиков — ротация общего пароля ноутбуков (юзер мог его видеть):
   новое значение `TRINO_ANALYST_SVC_PASSWORD` в `.env`, перегенерить строку
   `analyst_svc` в `password.db` (`htpasswd -nbB -C 10`, cost ≥ 8!),
   пересоздать jupyterhub. Живые ноутбуки получат пароль при следующем спавне.

## Как из ноутбука ходить в Trino

Переменные уже в контейнере (`TRINO_HOST/PORT/USER/PASSWORD` — общий
read-only аккаунт `analyst_svc`):

```python
import os, trino, pandas as pd

conn = trino.dbapi.connect(
    host=os.environ["TRINO_HOST"], port=int(os.environ["TRINO_PORT"]),
    user=os.environ["TRINO_USER"], http_scheme="https",
    auth=trino.auth.BasicAuthentication(
        os.environ["TRINO_USER"], os.environ["TRINO_PASSWORD"]),
)
df = pd.read_sql("SELECT * FROM iceberg.gold.dim_match LIMIT 10", conn)
```

Хочешь, чтобы запросы были подписаны твоим именем (видно в query history):

```python
conn = trino.dbapi.connect(
    host=os.environ["TRINO_HOST"], port=int(os.environ["TRINO_PORT"]),
    user="<твой-логин>", http_scheme="https",
    auth=trino.auth.OAuth2Authentication(),  # напечатает ссылку — кликни и войди
)
```

## DBeaver / JDBC

```
jdbc:trino://trino.sk-vpn-2026.uk:443?SSL=true&externalAuthentication=true
```

При подключении откроется браузер с единым логином. Логин в поле user
должен совпадать с логином Keycloak.

## Админ-доступ (SSH-туннель)

`/admin` и `/realms/master` Keycloak снаружи закрыты (403). Админское —
через туннель:

```bash
ssh -L 8180:127.0.0.1:8180 -L 8585:127.0.0.1:8585 root@159.195.193.250
```

- Keycloak-админка: `http://127.0.0.1:8180/admin/` (KC_HOSTNAME_ADMIN
  в compose направляет весь флоу консоли на этот адрес).
- OpenMetadata: обычный вход — публичный `https://meta.sk-vpn-2026.uk`;
  туннель `http://127.0.0.1:8585` остаётся как break-glass (вход локальным
  `admin` работает только при откате в basic-режим).

## Защита публичного входа

- Keycloak: brute-force protection в realm (lockout по юзеру).
- fail2ban на хосте по JSON-логу Caddy (`/var/log/caddy/access.log`):
  бёрсты 401/403 и частые POST на форму логина KC → бан IP в DOCKER-USER.
  Конфиги: `configs/fail2ban/` (деплой — `cp` в `/etc/fail2ban/`).
- Пароли машинных аккаунтов Trino: 48 символов, bcrypt cost 10.

## Если Keycloak лежит (break-glass)

Пайплайны не зависят от Keycloak и продолжают работать. Для входа людей:

- Airflow: `AIRFLOW_OAUTH_ENABLED=false` в `.env` →
  `docker compose up -d --no-deps airflow-webserver` → форма пароля (локальный admin).
- Superset: `SUPERSET_OAUTH_ENABLED=false` → `docker compose up -d --no-deps superset`.
- Trino: машинные аккаунты и `analyst_svc` работают по password.db всегда.
- OpenMetadata: закомментировать блок `OM_AUTH_*` в `.env` (ключи `OM_RSA_*`
  оставить!) + закомментировать meta-блок Caddyfile (`caddy reload`) →
  `docker compose --profile heavy up -d openmetadata-server` → basic-вход
  локальным `admin` через SSH-туннель. Ingestion (бот-JWT) не зависит от
  Keycloak и работает всё это время.
- Админ-доступ по SSH-туннелям (127.0.0.1-порты) никуда не делся.
