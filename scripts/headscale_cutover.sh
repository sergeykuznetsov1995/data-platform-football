#!/bin/bash
# Cutover на Headscale-гибрид (docs/HEADSCALE_MIGRATION.md §2) одним прогоном.
# Запускать на VM из корня репо. Пайплайны не трогает; веб-вход людей
# недоступен ~5 минут. Rollback — §4 ранбука.
set -euo pipefail
cd "$(dirname "$0")/.."

compose() { ./scripts/compose.sh --env-file .env "$@"; }

D=$(grep '^PLATFORM_DOMAIN=' .env | cut -d= -f2-)
HS_SECRET=$(grep '^HEADSCALE_OIDC_CLIENT_SECRET=' .env | cut -d= -f2-)
KC_ADMIN=$(grep '^KC_BOOTSTRAP_ADMIN_USERNAME=' .env | cut -d= -f2-); KC_ADMIN=${KC_ADMIN:-admin}
KC_PASS=$(grep '^KC_BOOTSTRAP_ADMIN_PASSWORD=' .env | cut -d= -f2-)
[ -n "$D" ] && [ -n "$HS_SECRET" ] && [ -n "$KC_PASS" ] || { echo "ERROR: .env не заполнен"; exit 1; }
echo "== Домен: $D"

kcadm() { compose exec -T keycloak /opt/keycloak/bin/kcadm.sh "$@"; }
client_id() {
    kcadm get clients -r football -q "clientId=$1" --fields id \
        | python3 -c 'import json,sys; print(json.load(sys.stdin)[0]["id"])'
}

echo "== 1/8 Keycloak: клиент headscale + новые redirect-URI"
kcadm config credentials --server http://localhost:8080 --realm master \
    --user "$KC_ADMIN" --password "$KC_PASS" >/dev/null
kcadm get clients -r football -q clientId=headscale --fields id | grep -q '"id"' || \
    kcadm create clients -r football \
        -s clientId=headscale -s name=Headscale -s enabled=true -s publicClient=false \
        -s clientAuthenticatorType=client-secret -s "secret=$HS_SECRET" \
        -s standardFlowEnabled=true -s implicitFlowEnabled=false \
        -s directAccessGrantsEnabled=false -s serviceAccountsEnabled=false \
        -s "redirectUris=[\"https://hs.$D/oidc/callback\"]" \
        -s 'defaultClientScopes=["profile","email","basic","groups"]'
kcadm update "clients/$(client_id airflow)" -r football \
    -s "redirectUris=[\"https://airflow.$D/oauth-authorized/keycloak\"]" -s "webOrigins=[\"https://airflow.$D\"]"
kcadm update "clients/$(client_id superset)" -r football \
    -s "redirectUris=[\"https://bi.$D/oauth-authorized/keycloak\"]" -s "webOrigins=[\"https://bi.$D\"]"
kcadm update "clients/$(client_id jupyterhub)" -r football \
    -s "redirectUris=[\"https://jupyter.$D/hub/oauth_callback\"]" -s "webOrigins=[\"https://jupyter.$D\"]"
kcadm update "clients/$(client_id trino)" -r football \
    -s "redirectUris=[\"https://trino.$D/oauth2/callback\"]" -s "webOrigins=[\"https://trino.$D\"]"

echo "== 2/8 .env: cutover-переключатели"
grep -q '^OIDC_ISSUER=' .env || cat >> .env <<EOF
OIDC_ISSUER=https://auth.$D/realms/football
KC_PUBLIC_URL=https://auth.$D
JUPYTER_PUBLIC_HOST=jupyter.$D
TRINO_PUBLIC_HOST=trino.$D
TRINO_PUBLIC_PORT=443
EOF

echo "== 3/8 Caddy: гибридный роутинг + Keycloak: новый issuer"
cp configs/caddy/Caddyfile.hybrid.example configs/caddy/Caddyfile
compose up -d --no-deps --force-recreate keycloak caddy
ISS=""
for i in $(seq 1 30); do
    ISS=$(curl -sk "https://auth.$D/realms/football/.well-known/openid-configuration" 2>/dev/null \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("issuer",""))' 2>/dev/null || true)
    [ "$ISS" = "https://auth.$D/realms/football" ] && { echo "   issuer OK (+серт LE)"; break; }
    sleep 5
done
[ "$ISS" = "https://auth.$D/realms/football" ] || { echo "ERROR: issuer не переключился"; exit 1; }

echo "== 4/8 Headscale: старт с OIDC"
compose restart headscale
for i in $(seq 1 12); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' headscale)" = healthy ] && { echo "   healthy"; break; }
    sleep 5
done
[ "$(docker inspect -f '{{.State.Health.Status}}' headscale)" = healthy ] || { echo "ERROR: headscale не поднялся"; exit 1; }

echo "== 5/8 VM входит в свой VPN"
compose exec -T headscale headscale users create platform 2>/dev/null || true
# в 0.29 --user принимает числовой ID, не имя
HS_UID=$(compose exec -T headscale headscale users list --output json \
    | python3 -c 'import json,sys; print(next(u["id"] for u in json.load(sys.stdin) if u["name"]=="platform"))')
KEY=$(compose exec -T headscale headscale preauthkeys create --user "$HS_UID" --expiration 1h --output json \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["key"])')
tailscale logout || true
# --snat-subnet-routes=false: иначе tailscale MASQUERADE'ит транзит в docker-bridge
# и Caddy видит 172.x вместо 100.64.x — VPN-гейт (remote_ip) отдаёт 403
tailscale up --login-server "https://hs.$D" --authkey "$KEY" --accept-dns=false --snat-subnet-routes=false
NEWIP=$(tailscale ip -4)
echo "   headscale-IP VM: $NEWIP"

echo "== 6/8 .env: новые TS_IP/TS_HOSTNAME + split-DNS"
sed -i "s|^TS_IP=.*|TS_IP=$NEWIP|" .env
sed -i "s|^TS_HOSTNAME=.*|TS_HOSTNAME=386844.tail.$D|" .env
make render-headscale-config
PUB=$(grep '^PUBLIC_IP=' .env | cut -d= -f2-)
NEWIP="$NEWIP" PUB="$PUB" D="$D" python3 - <<'PYEOF'
import os
ip, pub, d = os.environ["NEWIP"], os.environ["PUB"], os.environ["D"]
p = "configs/headscale/config.yaml"
recs = "extra_records:\n" + "\n".join(
    [f'    - {{ name: "{n}.{d}", type: "A", value: "{ip}" }}'
     for n in ["jupyter", "airflow", "trino", "meta"]]
    # bi/auth/hs — публичный IP явно, чтобы split не форвардил их (SERVFAIL)
    + [f'    - {{ name: "{n}.{d}", type: "A", value: "{pub}" }}'
       for n in ["bi", "auth", "hs"]])
t = open(p).read()
assert "extra_records: []" in t, "extra_records не найден"
open(p, "w").write(t.replace("extra_records: []", recs))
print("   extra_records OK (4 VPN + 3 public)")
PYEOF
compose restart headscale

echo "== 7/8 Пересоздание сервисов с новым issuer"
compose up -d --no-deps --force-recreate \
  caddy trino superset airflow-webserver jupyterhub

echo "== 8/8 Проверки"
sleep 20
printf 'bi из интернета (ждём 302 на вход): '
curl -sk -o /dev/null -w '%{http_code}\n' "https://bi.$D/login/"
printf 'jupyter из интернета (ждём 403): '
PUB=$(grep '^PUBLIC_IP=' .env | cut -d= -f2-)
curl -sk -o /dev/null -w '%{http_code}\n' --connect-to "jupyter.$D:443:$PUB:443" "https://jupyter.$D/"
printf 'trino через VPN-адрес (ждём {"...):  '
curl -sk "https://trino.$D/v1/info" --connect-to "trino.$D:443:$NEWIP:443" | head -c 40; echo
echo
echo "DONE. Перелогинь свой Mac в новый VPN:"
echo "  tailscale logout && tailscale up --login-server https://hs.$D"
echo "(откроется браузер — войди своим Keycloak-аккаунтом sergey)"
