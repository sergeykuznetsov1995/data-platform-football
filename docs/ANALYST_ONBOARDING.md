# Доступ юзеров к платформе

Дизайн и устройство: [design/analyst-access.md](design/analyst-access.md),
сеть — [HEADSCALE_MIGRATION.md](HEADSCALE_MIGRATION.md). Домен: `sk-vpn-2026.uk`.

## Два класса юзеров

| Класс (группа Keycloak) | Что получает | VPN нужен? |
|---|---|---|
| `viewers` — «простой» юзер | только Superset-дашборды | нет |
| `analysts` — аналитик | всё из таблицы ниже | да (Headscale) |

| Сервис | Адрес | Кому | Права |
|---|---|---|---|
| Superset | `https://bi.sk-vpn-2026.uk` | все | дашборды (viewers) + SQL Lab (analysts) |
| JupyterHub | `https://jupyter.sk-vpn-2026.uk` | analysts | свой контейнер (1 ГБ), диск |
| Airflow | `https://airflow.sk-vpn-2026.uk` | analysts | просмотр DAG'ов (Viewer) |
| Trino | `https://trino.sk-vpn-2026.uk` | analysts | SQL read-only `silver`/`gold` |

`bi`/`auth` — публичные (обычный интернет). `jupyter`/`airflow`/`trino`/`meta` —
только из VPN. Писать в данные нельзя: любой не-машинный юзер получает от
rules.json только SELECT на `silver`/`gold` (catch-all; правки конфигов на нового
юзера НЕ нужны). Запросы людей идут в ресурсной группе `humans` (лимит 8
одновременно, 30% памяти) и не могут задушить пайплайны.

## Онбординг

### Простой юзер (viewer) — ~1 минута, без VPN

1. `scripts/onboard_user.sh <username> <email>` (на VM) — заводит в Keycloak.
2. Отправь ссылку `https://bi.sk-vpn-2026.uk` + временный пароль из вывода скрипта.

### Аналитик — + доступ в VPN

1. **Headscale-клиент**: юзер ставит Tailscale-клиент
   (https://tailscale.com/download) и подключается к нашему серверу:
   ```bash
   tailscale up --login-server https://hs.sk-vpn-2026.uk --accept-dns=true
   ```
   Откроется браузер — вход тем же аккаунтом Keycloak (группа analysts).
   `--accept-dns=true` обязателен: сервер раздаёт split-DNS для
   `sk-vpn-2026.uk`, и VPN-имена резолвятся сами (без правки /etc/hosts —
   `jupyter/trino/...` → VPN, `bi/auth` → публично, всё из extra_records).
2. **Аккаунт** одной командой (создаёт юзера в Keycloak с временным паролем
   и кладёт в группу):

   ```bash
   scripts/onboard_user.sh <username> <email>            # viewer (default)
   scripts/onboard_user.sh <username> <email> analysts   # аналитик
   ```

   Скрипт напечатает временный пароль — отправь его юзеру вместе со ссылками.
   Viewers достаточно одной: `https://<host>:8088`.
3. Первый вход сам заведёт юзера в каждом сервисе с нужной ролью
   (viewers в JupyterHub/Airflow не пускаются).

Руками через админку (`https://<host>:8443/admin`, realm `football`) — тоже
можно: Users → Create user → Credentials (Temporary) → Groups → Join.

Админа платформы заводят так же (группа `platform-admins`), плюс строка
`platform-admins:` в `configs/trino/groups.txt` — это единственный случай,
когда groups.txt ещё нужен.

## Оффбординг

1. Keycloak → Users → Disable.
2. Tailscale-админка → удалить устройство юзера.
3. Только для аналитиков — ротация общего пароля ноутбуков (юзер мог его видеть):
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

## Если Keycloak лежит (break-glass)

Пайплайны не зависят от Keycloak и продолжают работать. Для входа людей:

- Airflow: `AIRFLOW_OAUTH_ENABLED=false` в `.env` →
  `docker compose up -d --no-deps airflow-webserver` → форма пароля (локальный admin).
- Superset: `SUPERSET_OAUTH_ENABLED=false` → `docker compose up -d --no-deps superset`.
- Trino: машинные аккаунты и `analyst_svc` работают по password.db всегда.
- Админ-доступ по SSH-туннелям (127.0.0.1-порты) никуда не делся.
