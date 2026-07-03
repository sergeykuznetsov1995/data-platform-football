# Миграция на Headscale + публичные дашборды (гибрид)

Что получится:

```
«Простой» юзер ── интернет ──► https://bi.<домен>   (Superset, вход через Keycloak)
Аналитик ──── Headscale VPN ──► jupyter/airflow/trino/meta.<домен>  (403 без VPN)
Keycloak ── https://auth.<домен> (публичный: на него редиректят все браузеры;
            /admin отдаётся только из VPN)
Headscale ── https://hs.<домен> (вход в VPN через тот же Keycloak-аккаунт)
```

Стоимость: домен (~$10/год). Tailscale-облако больше не нужно.
Машинные аккаунты и пайплайны не затрагиваются вообще (инвариант #860).

## 0. Предпосылки

1. Купить домен, далее `<домен>`.
2. DNS: A-записи `bi`, `auth`, `hs`, `jupyter`, `airflow`, `trino`, `meta`
   → публичный IP VM (все на публичный: это нужно Caddy для LE-сертификатов;
   доступ к VPN-именам всё равно режется по remote_ip).
3. В `.env` раскомментировать и заполнить блок «Гибрид» (см. `.env.example`):
   `PLATFORM_DOMAIN`, `PUBLIC_IP`, `HEADSCALE_OIDC_CLIENT_SECRET`,
   `COMPOSE_PROFILES=headscale`. Cutover-переключатели — ПОКА не заполнять.
4. Открыть на файрволе VM: tcp/80, tcp/443, udp/3478.
### VPN-гейт: bind-изоляция, НЕ remote_ip

Разделение «публичное / только-из-VPN» держится на том, на какой **хостовый
IP** опубликован порт Caddy, а не на `remote_ip` в Caddyfile. Причина:
docker при форвардинге tailscale0→bridge подменяет source-IP на bridge-gateway
(172.x) — даже с `userland-proxy: false`, — поэтому `remote_ip 100.64.0.0/10`
в Caddy НИКОГДА не матчит VPN-клиента (всё отдаёт 403). Проверено live.

Схема (см. `configs/caddy/Caddyfile.hybrid.example` + ports сервиса caddy):
- Caddy слушает два контейнерных порта: `:443` (публичные bi/auth/hs) и
  `:8443` (VPN jupyter/airflow/trino/meta — site-address с явным `:8443`).
- compose публикует: `${PUBLIC_IP}:443→:443`, `${PUBLIC_IP}:80→:80` (ACME),
  `${TS_IP}:443→:8443`.
- Клиенты ВЕЗДЕ ходят на `:443` (порт в URL не нужен). split-DNS (extra_records)
  резолвит VPN-сервисы в `${TS_IP}` → docker DNAT на контейнерный `:8443`;
  bi/auth/hs резолвятся публичным DNS в `${PUBLIC_IP}` → `:443`.
- Из интернета VPN-сервисов на `:443` нет (другой listener) → недоступны
  (Caddy отдаёт пустой 200, контент не проксируется).
- Серты LE — на все 7 доменов (A-записи → `${PUBLIC_IP}`, ACME HTTP-01 на :80);
  серт по домену, порт listener'а не важен.

## 1. Подготовка (без даунтайма, старый Tailscale продолжает работать)

```bash
make render-headscale-config
# клиент headscale в живом realm (import существующий realm не обновляет):
docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh config credentials \
  --server http://localhost:8080 --realm master --user admin --password "$KC_BOOTSTRAP_ADMIN_PASSWORD"
docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh create clients -r football \
  -s clientId=headscale -s enabled=true -s publicClient=false \
  -s clientAuthenticatorType=client-secret -s "secret=$HEADSCALE_OIDC_CLIENT_SECRET" \
  -s standardFlowEnabled=true -s 'redirectUris=["https://hs.<домен>/oidc/callback"]' \
  -s 'defaultClientScopes=["profile","email","basic","groups"]'
docker compose --profile headscale up -d headscale
docker compose up -d caddy   # подхватит PUBLIC_IP-порты; серты для bi/auth/hs
# юзер платформы в headscale появится при первом OIDC-логине; для VM нужен свой:
docker compose exec headscale headscale users create platform
docker compose exec headscale headscale preauthkeys create --user <ID из users list> --expiration 1h
```

Проверка: `https://hs.<домен>/health` отвечает из интернета с валидным сертом.

## 2. Cutover (окно ~30 мин; людям недоступно, пайплайны живут)

1. VM переходит в свой headscale (ts.net-имя и серт после этого умирают):

   ```bash
   tailscale logout
   tailscale up --login-server https://hs.<домен> --authkey <preauth-key из шага 1> \
       --accept-dns=false --snat-subnet-routes=false
   tailscale ip -4    # новый 100.x-адрес VM
   ```

   `--accept-dns=false` — VM-сервер не должен менять свой резолвер;
   `--snat-subnet-routes=false` — без него tailscale MASQUERADE'ит транзит
   в docker-bridge, Caddy видит 172.x вместо 100.64.x и VPN-гейт отдаёт 403.

2. `.env`: `TS_IP=<новый 100.x>`, `TS_HOSTNAME=386844.tail.<домен>`,
   раскомментировать cutover-переключатели (`OIDC_ISSUER`, `KC_PUBLIC_URL`,
   `JUPYTER_PUBLIC_HOST`, `TRINO_PUBLIC_HOST`, `TRINO_PUBLIC_PORT=443`).
3. Split-DNS: в `configs/headscale/config.yaml.example` раскомментировать
   `extra_records` (jupyter/airflow/trino/meta → новый 100.x VM),
   `make render-headscale-config && docker compose restart headscale`.
4. Caddy на гибридный роутинг:

   ```bash
   cp configs/caddy/Caddyfile.hybrid.example configs/caddy/Caddyfile
   ```

5. Redirect-URI клиентов в живом Keycloak (kcadm, по одному на клиента):
   - airflow    → `https://airflow.<домен>/oauth-authorized/keycloak`
   - superset   → `https://bi.<домен>/oauth-authorized/keycloak`
   - jupyterhub → `https://jupyter.<домен>/hub/oauth_callback`
   - trino      → `https://trino.<домен>/oauth2/callback`

   ```bash
   # id клиента: kcadm get clients -r football -q clientId=superset --fields id
   docker compose exec -T keycloak /opt/keycloak/bin/kcadm.sh update clients/<id> -r football \
     -s 'redirectUris=["https://bi.<домен>/oauth-authorized/keycloak"]' \
     -s 'webOrigins=["https://bi.<домен>"]'
   ```

6. Пересоздать сервисы (порядок важен: сначала keycloak и caddy):

   ```bash
   docker compose up -d keycloak caddy
   docker compose up -d trino superset airflow-webserver jupyterhub
   ```

7. Проверки:
   - из интернета: `https://bi.<домен>` — логин-редирект на `auth.<домен>`, вход работает;
     `https://jupyter.<домен>` — 403;
   - из VPN (ноутбук аналитика): jupyter/airflow открываются, спавн ноутбука,
     SELECT из ноутбука; DBeaver: `jdbc:trino://trino.<домен>:443?SSL=true&externalAuthentication=true`;
   - пайплайны: один Airflow-таск зелёный, дашборд Superset живой.

## 3. Онбординг после миграции

- **Viewer**: `scripts/onboard_user.sh <username> <email>` → ссылка `https://bi.<домен>`.
  Всё. Никакого VPN.
- **Аналитик**: то же + группа `analysts`; ставит Tailscale-клиент и выполняет
  `tailscale up --login-server https://hs.<домен>` → браузер → тот же
  Keycloak-логин → нода зарегистрирована сама (OIDC, allowed_groups
  analysts/platform-admins). Приглашений и одобрений в админке больше нет.
- **Оффбординг**: disable в Keycloak + `headscale nodes list` /
  `headscale nodes delete <id>`.

## 4. Rollback (если что-то пошло не так)

1. `.env`: закомментировать cutover-переключатели обратно (Caddyfile:
   `git checkout configs/caddy/Caddyfile`).
2. VM обратно в Tailscale-облако: `tailscale logout && tailscale up`
   (аккаунт decoy10215400@gmail), `tailscale ip -4` → вернуть старые
   `TS_IP`/`TS_HOSTNAME` в `.env`.
3. Вернуть redirect-URI клиентов в Keycloak (kcadm, старые ts.net-значения).
4. `docker compose up -d keycloak caddy trino superset airflow-webserver jupyterhub`.

Пайплайны всё это время работают: машинные аккаунты не зависят ни от VPN,
ни от issuer.

## 5. Гочи

- `--import-realm` НЕ обновляет существующий realm — клиент headscale и новые
  redirect-URI вносятся только kcadm/админкой (шаблон realm — для чистых стендов).
- После `tailscale logout` серт `*.ts.net` перестаёт продлеваться — возврата
  к старой схеме без шага rollback-2 нет.
- DERP требует валидного TLS на `hs.<домен>` — сначала шаг 1 (Caddy+серты),
  потом cutover.
- Контейнеры резолвят `auth.<домен>`/`trino.<домен>` в Caddy через
  network-алиасы (compose) — hairpin-NAT не нужен.
- Rate-limit/CrowdSec для публичных bi/auth — следующий шаг после миграции
  (Keycloak brute-force protection уже включён).
