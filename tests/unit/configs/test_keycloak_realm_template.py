"""Шаблон realm-импорта Keycloak (configs/keycloak/realm-football.json.example).

Шаблон рендерится scripts/render_keycloak_realm.py: каждый плейсхолдер
``__VAR__`` заменяется значением VAR из .env. Тесты держат три инварианта:

* шаблон — валидный JSON (плейсхолдеры не ломают структуру);
* каждый плейсхолдер имеет пару в .env.example (иначе рендер на чистом
  стенде упадёт «variable not set»);
* клиент ``openmetadata`` (#866) сконфигурирован как confidential-клиент
  с callback-путём OM ``/callback`` и groups-скоупом.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = PROJECT_ROOT / "configs" / "keycloak" / "realm-football.json.example"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"

# Тот же regex, что в scripts/render_keycloak_realm.py
PLACEHOLDER_RE = re.compile(r"__([A-Z0-9_]+)__")


@pytest.fixture(scope="module")
def realm() -> dict:
    return json.loads(TEMPLATE.read_text(encoding="utf-8"))


@pytest.mark.unit
class TestRealmTemplate:
    def test_template_is_valid_json(self, realm):
        assert realm["realm"] == "football"

    def test_every_placeholder_has_env_example_pair(self):
        placeholders = set(PLACEHOLDER_RE.findall(TEMPLATE.read_text(encoding="utf-8")))
        assert placeholders, "шаблон без плейсхолдеров — подозрительно"
        env_text = ENV_EXAMPLE.read_text(encoding="utf-8")
        env_vars = {
            line.split("=", 1)[0].lstrip("#")
            for line in env_text.splitlines()
            if "=" in line and not line.startswith("# ")
        }
        missing = placeholders - env_vars
        assert not missing, f"плейсхолдеры без пары в .env.example: {sorted(missing)}"

    def test_expected_groups_present(self, realm):
        names = {g["name"] for g in realm["groups"]}
        assert {"analysts", "viewers", "platform-admins"} <= names


@pytest.mark.unit
class TestOpenMetadataClient:
    @pytest.fixture(scope="class")
    def client(self, realm) -> dict:
        clients = {c["clientId"]: c for c in realm["clients"]}
        assert "openmetadata" in clients, "клиент openmetadata отсутствует (#866)"
        return clients["openmetadata"]

    def test_confidential_client_with_placeholder_secret(self, client):
        assert client["publicClient"] is False
        assert client["clientAuthenticatorType"] == "client-secret"
        assert client["secret"] == "__OPENMETADATA_OIDC_CLIENT_SECRET__"

    def test_authorization_code_flow_only(self, client):
        assert client["standardFlowEnabled"] is True
        assert client["implicitFlowEnabled"] is False
        assert client["directAccessGrantsEnabled"] is False
        assert client["serviceAccountsEnabled"] is False

    def test_redirect_uri_is_om_callback(self, client):
        # Callback-путь OpenMetadata — именно /callback (доки OM 1.13).
        assert client["redirectUris"] == ["https://meta.__PLATFORM_DOMAIN__/callback"]
        assert client["webOrigins"] == ["https://meta.__PLATFORM_DOMAIN__"]

    def test_groups_scope_included(self, client):
        assert "groups" in client["defaultClientScopes"]
