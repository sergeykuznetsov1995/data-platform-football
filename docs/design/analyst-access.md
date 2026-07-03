# Доступ аналитиков к платформе (multi-user: SSO, Jupyter, read-only Trino)

Статус: **реализовано** (фазы 0–8, деплой на VM). Дата: 2026-07-02. Runbook: ../ANALYST_ONBOARDING.md

## 1. Контекст и цель

Сейчас платформа — система «на одного»: все порты на `127.0.0.1` (вход только по SSH-туннелю),
в каждом сервисе один админ-аккаунт, в Trino три машинных логина (`airflow`, `superset`,
`openmetadata`) с полным write-доступом, `access-control` нет, Jupyter нет, реверс-прокси нет.

Хотим: аналитики (данные ~20 лиг) подключаются к платформе **одним логином**, работают в
Jupyter-ноутбуках, пишут SQL к Trino, смотрят дашборды в Superset и статусы DAG'ов в Airflow —
и при этом **не могут** сломать пайплайны или данные (только чтение silver/gold).

Принцип №1: **машинные аккаунты и пайплайны не трогаем** — они работают как работали,
на каждой фазе внедрения.

## 2. Решения по умолчанию (из интервью; каждое можно заменить)

Интервью не было завершено, взяты рекомендованные варианты. Если решение меняется —
меняется соответствующий раздел, остальной дизайн в основном переживает замену.

| # | Вопрос | Решение по умолчанию | Альтернативы (почему нет) |
|---|--------|----------------------|---------------------------|
| 1 | Сколько аналитиков | 2–5 сейчас, запас до ~10 | 15+ → нужна вторая VM под Jupyter |
| 2 | Как добираться до VM | **Tailscale VPN** | Публичный домен: удобнее юзерам, но сервисы торчат в интернет, вся защита на SSO. SSH-туннели: не масштабируются, больно объяснять |
| 3 | SSO | **Keycloak (OIDC)** | Authentik: проще админка, но меньше проверенных интеграций с Trino. Без SSO: каждый юзер = 4 ручных заведения |
| 4 | Что нужно аналитикам | JupyterHub + Trino read-only + Superset + Airflow (Viewer) | OpenMetadata — по запросу, basic auth |
| 5 | Формат работ | Сначала этот дизайн-док, код — по фазам отдельно | — |

## 3. Архитектура

```
Ноутбук аналитика (в tailnet, MagicDNS)
   │
   └── https://<vm>.<tailnet>.ts.net:<порт>      ← 1 hostname, роутинг по портам,
        │                                           настоящий LE-сертификат от Tailscale
        └── Caddy (порты опубликованы ТОЛЬКО на ${TS_IP};
             │     все текущие 127.0.0.1-биндинги остаются — админ ходит как раньше)
             ├── :443   → jupyterhub:8000        (новый сервис)
             ├── :8443  → keycloak:8080          (OIDC issuer, новый сервис)
             ├── :8444  → https://trino:8443     (re-encrypt: внутри self-signed, снаружи LE)
             ├── :8081  → airflow-webserver:8080
             ├── :8088  → superset:8088
             └── :8585  → openmetadata-server:8585 (опционально)
```

Ключевые решения и почему так:

1. **Tailscale + один hostname + порты, а не поддомены.** MagicDNS даёт одно имя на машину;
   поддомены потребовали бы свой DNS. Path-prefix-роутинг отвергнут: Superset и Keycloak
   за URL-префиксом — известная боль. Порты — скучно и надёжно.
2. **Сертификат — Let's Encrypt через `tailscale cert`**, а не `tls internal`.
   `tls internal` заставил бы раздавать CA в каждый браузер, в JVM-truststore Trino и во все
   python-клиенты. LE-серт от Tailscale доверен везде бесплатно.
3. **Главная OIDC-грабля — issuer URL.** Браузер аналитика и контейнеры (Trino, Airflow…)
   должны резолвить **один и тот же** адрес Keycloak. Решение: контейнер Caddy получает
   **docker network alias = tailnet-FQDN** на каждой сети. Изнутри докера имя резолвится
   в Caddy, снаружи — через MagicDNS. Один URL, один серт, обе стороны довольны.
4. **Два контура аутентификации.** Люди — через Keycloak (OIDC). Машины — как раньше,
   через `password.db` / локальных админов. Keycloak упал → пайплайны даже не заметят.

## 4. Компоненты

### 4.1 Keycloak (новый сервис)

- Образ: `quay.io/keycloak/keycloak:26.x` (пин точной версии + `@sha256` при реализации).
- Команда: `start --import-realm`.
- БД: новая база `keycloak` в общем postgres 16.
  Внимание: `docker/images/postgres/init-databases.sh` выполняется **только на свежем томе** —
  для прода нужен `make keycloak-db` (идемпотентный SQL через `docker compose exec postgres psql`).
- Память: лимит **1G**, heap `-Xmx512m` (правило проекта: heap ≤ 50–70 % лимита, иначе тихий OOM).
- Healthcheck: KC 25+ отдаёт `/health/ready` на management-порту **9000**, в образе нет curl/wget —
  проба через bash `/dev/tcp` (**проверить при реализации**).
- Сети: `backend` (postgres) + `frontend` (Caddy). Портов наружу нет; опционально
  `127.0.0.1:8180:8080` для админа через SSH-туннель.

Realm `football` (bootstrap из `configs/keycloak/realm-football.json.example`,
рендер с секретами из `.env` → gitignored `realm-football.json` — тот же паттерн,
что `configs/seaweedfs/s3.config.json`; на `${env.*}`-подстановку внутри import-JSON
**не** полагаемся — она нестабильна между версиями KC):

- Группы: `analysts`, `platform-admins`.
- Confidential-клиенты (секреты из `.env`):
  - `airflow`   → redirect `https://${TS_HOSTNAME}:8081/oauth-authorized/keycloak`
  - `superset`  → redirect `https://${TS_HOSTNAME}:8088/oauth-authorized/keycloak`
  - `jupyterhub`→ redirect `https://${TS_HOSTNAME}/hub/oauth_callback`
  - `trino`     → redirect `https://${TS_HOSTNAME}:8444/oauth2/callback`
- Client scope `groups` c Group Membership mapper (full path off) на всех клиентах —
  в каждом токене будет `"groups": ["analysts"]`. На этом claim держится вся авторизация ниже.

### 4.2 Ingress: Tailscale + Caddy

- На хост ставится `tailscaled`, `tailscale up`, в админке tailnet включаются MagicDNS и HTTPS.
  В `.env` добавляются `TS_HOSTNAME` (FQDN) и `TS_IP` (100.x-адрес).
- **Готча:** Docker не сможет опубликовать порт на `${TS_IP}`, если интерфейс tailscale
  ещё не поднят при буте → systemd drop-in: `docker.service` после `tailscaled.service`.
- Caddy: `image: caddy:2` (пин), сети `frontend`+`backend`+`analyst`, на каждой —
  `aliases: ["${TS_HOSTNAME}"]`. Все порты биндятся на `${TS_IP}`, host-порт == container-порт
  (чтобы `alias:8443` изнутри докера попадал на тот же листенер). Лимит 256M.
- Сертификат: предпочтительно нативная интеграция Caddy с Tailscale (монтируем
  `/var/run/tailscale/tailscaled.sock`, Caddy сам получает и продлевает серт;
  **проверить поддержку в версии Caddy**). Fallback: cron на хосте `tailscale cert`
  + монтирование файлов + явный `tls`-директив.
- Trino-сайт (`:8444`) проксирует на `https://trino:8443` с `tls_insecure_skip_verify`
  в transport — внутренний self-signed серт Trino остаётся, машинные клиенты не меняются.

### 4.3 Trino 482: аутентификация + авторизация

`configs/trino/config.properties` — добавить:

```properties
http-server.authentication.type=PASSWORD,OAUTH2
http-server.authentication.oauth2.issuer=https://<TS_HOSTNAME>:8443/realms/football
http-server.authentication.oauth2.client-id=trino
http-server.authentication.oauth2.client-secret=<из .env>
http-server.authentication.oauth2.scopes=openid
http-server.authentication.oauth2.principal-field=preferred_username
http-server.authentication.oauth2.groups-field=groups
web-ui.authentication.type=oauth2
```

- Порядок `PASSWORD,OAUTH2`: Basic-запросы (airflow/superset/OM/analyst_svc) обрабатывает
  PASSWORD — **машинные аккаунты не трогаются**; Bearer/браузер — OAUTH2.
- **Проверить при реализации:** поддержку `groups-field` в 482 и `${ENV:…}`-интерполяцию
  для issuer/секрета (если нет — рендерить config.properties из шаблона, как realm JSON).
- DBeaver/JDBC для людей: `jdbc:trino://<TS_HOSTNAME>:8444?SSL=true&externalAuthentication=true`
  → системный браузер → Keycloak. **Риск (UX зависит от версии DBeaver)**; fallback —
  персональные bcrypt-строки в `password.db` (файл перечитывается без рестарта).
- Новый общий read-only аккаунт **`analyst_svc`** в `password.db` (для ноутбуков, см. 4.6).

Новый `configs/trino/access-control.properties`:

```properties
access-control.name=file
security.config-file=/etc/trino/rules.json
security.refresh-period=30s
```

Новый `configs/trino/rules.json` (форма; **точную семантику file-based правил —
видимость схем, information_schema — проверить тестами на одноразовом Trino до прода**):

```json
{
  "catalogs": [
    {"user": "airflow|superset|openmetadata", "allow": "all"},
    {"group": "analysts", "catalog": "iceberg|system", "allow": "read-only"},
    {"user": "analyst_svc", "catalog": "iceberg|system", "allow": "read-only"},
    {"allow": "none"}
  ],
  "schemas": [
    {"user": "airflow|superset|openmetadata", "owner": true},
    {"owner": false}
  ],
  "tables": [
    {"user": "airflow|superset|openmetadata", "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE", "OWNERSHIP"]},
    {"group": "analysts", "catalog": "iceberg", "schema": "silver|gold", "privileges": ["SELECT"]},
    {"user": "analyst_svc", "catalog": "iceberg", "schema": "silver|gold", "privileges": ["SELECT"]},
    {"privileges": []}
  ],
  "impersonation": [
    {"original_user": "superset", "new_user": ".*"}
  ]
}
```

Смысл: машинные — всё; аналитики (по groups-claim из токена) и `analyst_svc` (по имени,
у password-логина claim'ов нет) — только SELECT на `iceberg.silver|gold`; bronze/ops невидимы.
`impersonation` — для Superset SQL Lab: запрос выполняется под личностью аналитика,
то есть read-only, а не под всемогущим `superset`.

Добавление аналитика №6 **не требует правок Trino** — группа приезжает в токене.

### 4.4 Airflow 2.11.2 (FAB)

Новый `configs/airflow/webserver_config.py`, монтируется read-only в
`x-airflow-common` → `/opt/airflow/webserver_config.py` (scheduler его игнорирует):

- `AUTH_TYPE = AUTH_OAUTH`; провайдер `keycloak` через
  `server_metadata_url = <issuer>/.well-known/openid-configuration`,
  scope `openid email profile groups`.
- `AUTH_USER_REGISTRATION = True`, `AUTH_USER_REGISTRATION_ROLE = "Viewer"`,
  `AUTH_ROLES_SYNC_AT_LOGIN = True`,
  `AUTH_ROLES_MAPPING = {"analysts": ["Viewer"], "platform-admins": ["Admin"]}`.
- Мини-security-manager: переопределить `get_oauth_user_info`, вернуть `role_keys` = groups
  из токена (**проверить точный хук в 2.11: `FabAirflowSecurityManagerOverride` +
  `SECURITY_MANAGER_CLASS`**). Худший случай: без маппинга, все входящие = Viewer,
  админов повышать руками.
- `authlib` → в `docker/images/airflow/requirements-airflow.txt`.
- **Break-glass:** env-guard внутри файла (`AIRFLOW_OAUTH_ENABLED != true` → `AUTH_DB`,
  форма логина с локальным админом возвращается). Рестарт — только webserver
  (`docker compose up -d --no-deps airflow-webserver`), scheduler не трогаем.

### 4.5 Superset 4.1.2

Дописать в `configs/superset/superset_config.py`, **под env-guard `SUPERSET_OAUTH_ENABLED`**
(rollback = флип env + рестарт):

- `AUTH_TYPE = AUTH_OAUTH`, тот же Keycloak-блок; `AUTH_USER_REGISTRATION_ROLE = "Gamma"`,
  `AUTH_ROLES_SYNC_AT_LOGIN = True`,
  `AUTH_ROLES_MAPPING = {"analysts": ["Gamma", "sql_lab", "analyst_data"], "platform-admins": ["Admin"]}`.
- `CUSTOM_SECURITY_MANAGER` — сабкласс `SupersetSecurityManager` с `oauth_user_info`
  (groups → role_keys).
- Роль `analyst_data` (идемпотентно в `configs/superset/bootstrap.sh`): доступ к
  Trino-подключению, чтобы Gamma видел gold-датасеты и SQL Lab.
- На Trino-подключении включить **"Impersonate the logged in user"** — SQL Lab работает
  под аналитиком, Trino режет права по `rules.json` (**проверить связку
  trino-sqlalchemy impersonation**; fallback: убрать `sql_lab` из маппинга,
  SQL — только через Jupyter/DBeaver).
- `Authlib` → в `docker/images/superset/Dockerfile`
  (каталог отсутствует на текущей ветке — восстановить из `e1f95e0`, см. фазу 0).

### 4.6 JupyterHub (новый сервис)

Два новых образа + конфиг:

- `docker/images/jupyterhub/Dockerfile`: `FROM quay.io/jupyterhub/jupyterhub:5.x`
  + `pip install oauthenticator dockerspawner jupyterhub-idle-culler`.
- `docker/images/jupyter-singleuser/Dockerfile`: `FROM quay.io/jupyter/minimal-notebook:<пин>`
  + `pip install trino[sqlalchemy] pyiceberg[pyarrow] pandas matplotlib ipywidgets`.
- `configs/jupyterhub/jupyterhub_config.py`:
  - `GenericOAuthenticator`: client `jupyterhub`, `username_claim="preferred_username"`,
    `claim_groups_key="groups"`, `allowed_groups={"analysts"}`, `admin_groups={"platform-admins"}`.
  - `DockerSpawner`: образ singleuser, сеть `dp-analyst`, `remove=True`, `mem_limit="2G"`,
    `cpu_limit=2`, персональный named volume `jupyterhub-user-{username}` → `/home/jovyan/work`.
  - Env для Trino в спавнере: `TRINO_HOST=${TS_HOSTNAME}`, `TRINO_PORT=8444`,
    `TRINO_USER=analyst_svc`, `TRINO_PASSWORD=…` — ноутбуки ходят **через Caddy** с настоящим
    сертом, никаких `verify=False`. Общий read-only аккаунт — осознанная простота;
    per-user вариант (`trino.auth.OAuth2Authentication()`, кликабельная ссылка на Keycloak
    прямо в ноутбуке) — документированная опция для тех, кому нужна атрибуция запросов.
  - idle-culler (`--timeout=3600`) + `c.JupyterHub.active_server_limit = 6` — защита RAM.
  - БД хаба: sqlite на named volume (на этом масштабе — норм).
- Compose: сети `frontend` + `analyst`; лимит 512M; healthcheck `/hub/health`;
  монтируется `/var/run/docker.sock` (нужен DockerSpawner). **Риск:** docker.sock =
  root на хосте; для v1 с 2–5 доверенными юзерами принято, опция — `docker-socket-proxy`.
- Новая сеть **`analyst`** (`name: dp-analyst`): hub, юзер-контейнеры, Caddy (с алиасом).
  Юзер-контейнеры **не видят** postgres/redis/lakekeeper/seaweedfs.

### 4.7 Что осознанно отложено

- **OpenMetadata → OIDC**: смена `AUTHENTICATION_PROVIDER` инвазивна (bot-JWT, migrate,
  admin-principals). Оставляем basic auth; редким желающим — ручные аккаунты.
- **Lakekeeper**: работает с `security=NONE` → любой сетевой доступ = неаутентифицированный
  write мимо Trino-ACL. Поэтому pyiceberg в образе есть, но Lakekeeper к сети `dp-analyst`
  **не подключён**. Включение OIDC в Lakekeeper (+ `iceberg.rest-catalog.security=OAUTH2`
  в Trino) — отдельный эпик в `docs/SECURITY_TODO.md`.

## 5. Бюджет памяти (VM 62 ГБ)

| Группа | Состав | Лимиты |
|---|---|---|
| Существующее (core) | seaweedfs 1G, lakekeeper 0.5G, postgres 1G, redis 0.13G, scheduler 8G, webserver 2G (1G→2G в фазе 6: memcg-OOM воркеров с SSO), trino 8G, superset 3G (1.5G→3G под 4 gunicorn-воркера, issue #861), flaresolverr 2G, proxy_filter 0.25G | ~25.9G |
| Существующее (heavy) | opensearch 2G, om-server 2G, om-ingestion 0.8G, superset-worker 0.5G, -beat 0.2G, tor 0.25G | ~5.8G |
| Новое | keycloak 1G, caddy 0.25G, jupyterhub 0.5G | 1.75G |
| Ноутбуки | лимит 12 активных × 1G (issue #861; было 6×2G) | 12G |
| **Итого** | всё включено + все 12 ноутбуков | **~45.5G / 62G** |

Postgres получает лёгкого жильца (Keycloak) — лимит 1G остаётся, наблюдать на фазе 2.

## 6. Фазы внедрения

Всё в ветке `feat/analyst-access`, деплой на VM — вручную по фазам.
Инвариант каждой фазы: машинные аккаунты Trino и пайплайны работают.

| Фаза | Что | Проверка |
|---|---|---|
| **0. Reconcile + подготовка** | Свериться с прод-контейнерами (`docker inspect`): ветка дрейфанула (нет `docker/images/superset/`, airflow Dockerfile «2.7.3» при проде 2.11.2) — отрезать feature-ветку от состояния, совпадающего с продом. Ротация placeholder-паролей Trino из SECURITY_TODO (тихое окно) | `docker compose config -q`; дашборды Superset и один Airflow-таск зелёные после ротации |
| **1. Tailscale + Caddy** | tailscaled на хост + systemd-ordering; caddy публикуется только на `${TS_IP}` | С ноутбука в tailnet `:8081/:8088/:8444` открываются с валидным сертом; из контейнера trino `curl https://${TS_HOSTNAME}:8443` резолвится в Caddy; SSH-туннели работают как раньше |
| **2. Keycloak** | `make keycloak-db` → рендер realm → up | Вход в админку; тестовый аналитик; `.well-known/openid-configuration` доступен и с ноутбука, И из контейнера trino; в токене есть `groups`; память postgres в норме |
| **3. Trino authz** | `analyst_svc` в password.db; access-control + rules.json. Сначала — **одноразовый второй trino-контейнер** с теми же конфигами | На одноразовом: `airflow` может CTAS в bronze; `analyst_svc` SELECT silver/gold — да, INSERT — deny, bronze — невидим. Потом прод-рестарт в тихое окно + прогон DAG-таска, дашборда, OM-ingestion |
| **4. Trino OAuth2** | `PASSWORD,OAUTH2` + oauth2-блок + web-ui oauth2; рестарт trino | Superset-дашборды живы (Basic-путь); web UI `:8444` редиректит в SSO; DBeaver externalAuthentication; аналитик read-only. Rollback = вернуть `PASSWORD` (одна строка) |
| **5. JupyterHub** | Собрать 2 образа, up hub | Логин тестовым аналитиком; запрос к silver из ноутбука через `:8444`; INSERT падает; culler убивает idle-контейнер |
| **6. Airflow SSO** | webserver_config.py, рестарт **только webserver** | Аналитик авто-регистрируется Viewer'ом, DAG триггерить не может; break-glass toggle проверен один раз |
| **7. Superset SSO** | `SUPERSET_OAUTH_ENABLED=true`, рестарт superset | Аналитик = Gamma+analyst_data, видит gold-дашборды; SQL Lab под impersonation режет write; rollback-toggle проверен |
| **8. Документация** | `docs/ANALYST_ONBOARDING.md`, обновить SECURITY_TODO (ротация ✓, OM/Lakekeeper-OIDC — отложенные эпики), опционально OM через Caddy | — |

## 7. Runbook: добавить аналитика №6

1. Tailscale-админка: пригласить юзера / одобрить девайс (~1 мин).
2. Keycloak (`https://<TS_HOSTNAME>:8443`) → realm `football` → Users → Create →
   временный пароль (required action «Update Password») → Groups → `analysts` (~2 мин).
3. Отправить ссылки: Jupyter `https://<TS_HOSTNAME>/`, Superset `:8088`, Airflow `:8081`,
   DBeaver `jdbc:trino://<TS_HOSTNAME>:8444?SSL=true&externalAuthentication=true`.

Ноль правок конфигов, ноль рестартов. Первый логин авто-провижнит: Jupyter (группа),
Airflow (Viewer), Superset (Gamma+analyst_data), Trino (read-only по groups-claim).

Offboarding: disable юзера в Keycloak + убрать девайс из tailnet. Плюс ротация общего
`analyst_svc` (одна `htpasswd -B`-строка + env спавнера; новые контейнеры подхватят).

## 8. Риски и fallback'и

| # | Риск | Fallback |
|---|------|----------|
| 1 | `oauth2.groups-field` в Trino 482 (**проверить**) | file group-provider (`group-provider.properties` + `groups.txt`) — цена: одна строка в файле на нового юзера |
| 2 | DBeaver + externalAuthentication UX (**проверить версию**) | персональные записи в password.db + персональные строки в rules.json |
| 3 | Семантика rules.json: видимость схем, information_schema (**проверить**) | итерировать на одноразовом trino; непроверенное в прод не едет |
| 4 | Superset impersonation → Trino (**проверить**) | убрать `sql_lab` у аналитиков; SQL — через Jupyter/DBeaver |
| 5 | Healthcheck Keycloak (нет curl, mgmt-порт 9000) (**проверить**) | `/dev/tcp`-проба или только start_period |
| 6 | Caddy ↔ Tailscale-серт нативно (**проверить**) | cron `tailscale cert` + файловый `tls` |
| 7 | Docker публикует на `${TS_IP}` раньше tailscaled при буте | systemd drop-in ordering (фаза 1) |
| 8 | Дрейф worktree ↔ прод (урок Tier-2) | фаза 0 — жёсткий пререквизит |
| 9 | docker.sock у JupyterHub | принято для v1 (доверенные 2–5 юзеров); опция docker-socket-proxy |
| 10 | Lakekeeper без auth + pyiceberg | каталог не подключён к `dp-analyst`; Lakekeeper-OIDC — отложенный эпик |
| 11 | Хук security-manager'а в Airflow 2.11 (**проверить**) | без маппинга ролей: все = Viewer, админов повышать руками |
| 12 | Keycloak упал → люди не логинятся | пайплайны живут (password.db); break-glass env-toggle в Airflow/Superset; SSH-туннель + локальные админы остаются |

## 9. Новые / изменяемые файлы

Новые:

- `configs/keycloak/realm-football.json.example` (+ gitignore на рендер)
- `configs/caddy/Caddyfile`
- `configs/trino/access-control.properties`, `configs/trino/rules.json`
- `configs/airflow/webserver_config.py`
- `configs/jupyterhub/jupyterhub_config.py`
- `docker/images/jupyterhub/Dockerfile`, `docker/images/jupyter-singleuser/Dockerfile`
- `docs/ANALYST_ONBOARDING.md`
- `scripts/render_keycloak_realm.py` (или envsubst в Makefile)

Изменяемые:

- `compose.yaml`: +keycloak, +caddy, +jupyterhub; +сеть `analyst`; +тома `caddy_data`,
  `jupyterhub_data`; env для trino/superset/airflow
- `configs/trino/config.properties` (auth-типы + oauth2), `configs/trino/password.db`
  (+`analyst_svc`, ротация машинных хэшей)
- `configs/superset/superset_config.py` (env-gated OAuth), `configs/superset/bootstrap.sh`
  (+роль `analyst_data`)
- `docker/images/airflow/requirements-airflow.txt` (+authlib),
  `docker/images/superset/Dockerfile` (+Authlib, после восстановления каталога)
- `docker/images/postgres/init-databases.sh` (+база keycloak) + `make keycloak-db` для прода
- `.env.example`: `TS_HOSTNAME`, `TS_IP`, `KEYCLOAK_DB_PASSWORD`,
  `KC_BOOTSTRAP_ADMIN_USERNAME/PASSWORD`, `AIRFLOW_OIDC_CLIENT_SECRET`,
  `SUPERSET_OIDC_CLIENT_SECRET`, `JUPYTERHUB_OIDC_CLIENT_SECRET`, `TRINO_OIDC_CLIENT_SECRET`,
  `TRINO_ANALYST_SVC_PASSWORD`, `SUPERSET_OAUTH_ENABLED=false`
- `Makefile`: `keycloak-db`, `render-keycloak-realm`, `build-jupyter`, `trino-acl-test`,
  `logs-keycloak`, `logs-jupyterhub`
- `docs/SECURITY_TODO.md`: ротация ✓; отложенные эпики OM-OIDC и Lakekeeper-OIDC

## 10. Масштаб ~100 юзеров (issue #861, 2026-07-03)

Вводная: скоро ~100 юзеров (большинство — «простые», только Superset-дашборды;
аналитиков с Jupyter/SQL — 10–20), VM та же. Что изменено:

1. **Ресурсные группы Trino** (`resource-groups.properties` + `resource-groups.json`,
   нужен рестарт Trino): машинные аккаунты и `platform-admins` (по userGroup из
   groups.txt) — группа `machine` без практических лимитов; все остальные люди —
   `humans` (8 одновременных запросов, 30% памяти, очередь 50). Проверено на
   одноразовом Trino: 12 параллельных запросов → RUNNING=8, QUEUED=4, пайплайны
   не задеты.
2. **Catch-all в rules.json**: любой не-машинный юзер = SELECT только
   `iceberg.silver|gold` (bronze/ops и каталог lakekeeper невидимы — проверено
   15 тестами). Строка в groups.txt на юзера больше НЕ нужна (осталась только
   `platform-admins:`). Онбординг = один шаг в Keycloak.
3. **Класс `viewers`**: группа в Keycloak (в живой realm добавить руками/kcadm —
   `--import-realm` существующий realm не обновляет), маппинг в Superset
   `viewers → Gamma+analyst_data` (дашборды без SQL Lab); в JupyterHub/Airflow
   viewers не пускаются (allowed_groups/маппинги не включают их).
4. **JupyterHub**: 1G/1cpu на ноутбук, active_server_limit 12, culler 30 мин.
5. **Superset**: 4 gunicorn-воркера (`SERVER_WORKER_AMOUNT`), лимит 3G.
6. **`scripts/onboard_user.sh`**: юзер+temp-пароль+группа одной командой (kcadm).

### Сеть при 100 юзерах (решение НЕ принято — за владельцем)

Tailscale free теперь 6 юзеров; дальше Standard ~$8/юзер/мес (100 юзеров ≈
$800/мес). Бесплатные альтернативы (ресёрч 2026-07):

- **Рекомендация (1-е место): гибрид** — Superset+Keycloak публикуются на
  публичный домен через Caddy (простым юзерам ноль установки, поверхность
  атаки минимальна: оба уже за SSO с brute-force protection; чек-лист:
  фиксированный KC_HOSTNAME, /admin закрыть по IP, CrowdSec/fail2ban,
  rate-limit, обновления Keycloak); аналитикам (10–20) — **Headscale**
  (self-hosted control-plane, официальные Tailscale-клиенты, один бинарь).
  Гоча: OIDC issuer один для всех → Keycloak обязан быть публичным, а
  контейнеры должны резолвить публичный FQDN в Caddy (alias/extra_hosts —
  тот же приём, что с tailnet-FQDN).
- 2-е место: тот же гибрид, но аналитикам платный Tailscale ($80–160/мес).
- 3-е место: NetBird self-hosted для всех (бесплатно, логин через наш Keycloak,
  но 100 «простых» ставят клиент + 4 компонента ops).
- Отпадают: ZeroTier (free 10 устройств, $2/устройство), Cloudflare Access
  (free 50 seats, дальше $7/юзер за всех; JDBC не дружит без WARP).
