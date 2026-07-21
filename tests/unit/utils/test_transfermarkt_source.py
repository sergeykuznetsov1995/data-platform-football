from types import SimpleNamespace

import pytest

from dags.utils import transfermarkt_source as source


def test_source_contract_is_native_only_and_complete():
    report = source.validate_source_contracts()

    assert report == {'passed': True, 'relation_count': 11, 'entity_count': 7}
    assert not (
        set(source.SOURCE_CONTRACT_BY_RELATION)
        & source.FORBIDDEN_LEGACY_RELATIONS
    )
    assert source.PARTICIPANTS_TABLE in source.SOURCE_CONTRACT_BY_RELATION
    assert source.RAW_RESPONSES_TABLE in source.SOURCE_CONTRACT_BY_RELATION


def test_supported_endpoints_explicitly_exclude_match_surfaces():
    assert source.SUPPORTED_ENDPOINTS
    assert not any('match' in endpoint for endpoint in source.SUPPORTED_ENDPOINTS)


def test_tls_bundle_must_be_readable_absolute_file(monkeypatch, tmp_path):
    monkeypatch.setenv('TRINO_TLS_CA_BUNDLE', 'relative.pem')
    with pytest.raises(RuntimeError, match='readable absolute CA file'):
        source._tls_verify_value()

    missing = tmp_path / 'missing.pem'
    monkeypatch.setenv('TRINO_TLS_CA_BUNDLE', str(missing))
    with pytest.raises(RuntimeError, match='readable absolute CA file'):
        source._tls_verify_value()

    bundle = tmp_path / 'ca.pem'
    bundle.write_text('certificate', encoding='utf-8')
    monkeypatch.setenv('TRINO_TLS_CA_BUNDLE', str(bundle))
    assert source._tls_verify_value() == str(bundle)


def test_connect_enables_certificate_verification(monkeypatch, tmp_path):
    bundle = tmp_path / 'ca.pem'
    bundle.write_text('certificate', encoding='utf-8')
    monkeypatch.setenv('TRINO_HOST', 'trino.internal')
    monkeypatch.setenv('TRINO_PORT', '8443')
    monkeypatch.setenv('TRINO_USER', 'airflow')
    monkeypatch.setenv('TRINO_PASSWORD', 'secret')
    monkeypatch.setenv('TRINO_TLS_CA_BUNDLE', str(bundle))

    captured = {}

    class BasicAuthentication:
        def __init__(self, user, password):
            self.user = user
            self.password = password

    def fake_connect(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace()

    fake_trino = SimpleNamespace(
        dbapi=SimpleNamespace(connect=fake_connect),
        auth=SimpleNamespace(BasicAuthentication=BasicAuthentication),
    )
    monkeypatch.setitem(__import__('sys').modules, 'trino', fake_trino)
    monkeypatch.setitem(__import__('sys').modules, 'trino.auth', fake_trino.auth)

    source.connect()

    assert captured['verify'] == str(bundle)
    assert captured['http_scheme'] == 'https'
    assert captured['auth'].user == 'airflow'
