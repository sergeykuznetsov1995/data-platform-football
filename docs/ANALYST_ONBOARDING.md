# Доступ аналитика к платформе

Дизайн и устройство: [design/analyst-access.md](design/analyst-access.md).
Хост платформы (tailnet): `386844.tailb2c32a.ts.net` — далее `<host>`.

## Что получает аналитик

Один аккаунт (Keycloak, группа `analysts`) открывает:

| Сервис | Адрес | Права |
|---|---|---|
| JupyterHub | `https://<host>/` | свой контейнер (2 ГБ), персональный диск |
| Superset | `https://<host>:8088` | дашборды + SQL Lab (read-only) |
| Airflow | `https://<host>:8081` | просмотр DAG'ов (Viewer) |
| Trino | `https://<host>:8444` | SQL read-only к `silver`/`gold`; `bronze` невидим |

Писать в данные нельзя — это гарантирует Trino, а не вежливость.

## Онбординг нового аналитика (~5 минут, без рестартов)

1. **Tailscale**: пригласи юзера в tailnet (админка → Users → Invite) —
   он ставит клиент с https://tailscale.com/download и логинится.
2. **Keycloak** `https://<host>:8443/admin` (realm `football`):
   Users → Create user (username, email) → Credentials → временный пароль
   (галка Temporary) → Groups → Join `analysts`.
3. **Trino-группы**: добавь username в строку `analysts:` файла
   `configs/trino/groups.txt` (подхватывается за 30 секунд, рестарт не нужен).
   Без этого шага SQL Lab в Superset не даст прав на данные.
4. Отправь юзеру ссылки из таблицы выше. Первый вход сам заведёт его
   в каждом сервисе с нужной ролью.

Админа платформы заводят так же, но группа `platform-admins`
и строка `platform-admins:` в groups.txt.

## Оффбординг

1. Keycloak → Users → Disable.
2. Tailscale-админка → удалить устройство юзера.
3. Убрать username из `configs/trino/groups.txt`.
4. Ротация общего пароля ноутбуков (юзер мог его видеть):
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
jdbc:trino://<host>:8444?SSL=true&externalAuthentication=true
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
