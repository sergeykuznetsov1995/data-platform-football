from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "scripts" / "migrate_proxy_pool_secret.py"


def _load():
    spec = importlib.util.spec_from_file_location("migrate_proxy_pool_secret", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_colon_and_compose_sensitive_password_round_trip():
    module = _load()
    password = "p:a$#\\ss'word"
    records = module.parse_legacy_pool(
        f"Pool.Example:10000:user:{password}\n".encode(),
    )
    canonical = module.canonical_pool_json(records)

    assert "'" not in canonical
    assert "$" not in canonical
    assert json.loads(canonical)[0]["password"] == password
    assert records[0]["host"] == "pool.example"


def test_invalid_and_duplicate_inputs_are_redacted():
    module = _load()
    secret = "do-not-echo-this-secret"
    with pytest.raises(module.SecretMigrationError) as malformed:
        module.parse_legacy_pool(f"host:1000:user:{secret}\nhost:bad\n".encode())
    assert secret not in str(malformed.value)

    with pytest.raises(module.SecretMigrationError, match="duplicate") as duplicate:
        module.parse_legacy_pool(
            f"host.example:1000:user:{secret}\n"
            f"host.example:1000:user:{secret}\n".encode(),
        )
    assert secret not in str(duplicate.value)


def test_plan_is_side_effect_free_and_apply_is_hash_bound(tmp_path):
    module = _load()
    source = tmp_path / "proxys.txt"
    output = tmp_path / ".env.proxy-pool"
    source.write_text("pool.example:10000:user:p:a:ss\n", encoding="utf-8")

    plan = module.build_plan(source, output)
    assert plan["entry_count"] == 1
    assert not output.exists()
    with pytest.raises(module.SecretMigrationError, match="drift"):
        module.apply_plan(
            plan,
            expected_sha256="0" * 64,
            expected_plan_sha256=plan["plan_sha256"],
        )
    assert not output.exists()

    result = module.apply_plan(
        plan,
        expected_sha256=plan["canonical_sha256"],
        expected_plan_sha256=plan["plan_sha256"],
    )
    assert result["status"] == "applied"
    assert stat_mode(output) == 0o600
    line = output.read_text(encoding="utf-8").rstrip("\n")
    assert line.startswith("PROXY_POOL_JSON='") and line.endswith("'")
    assert json.loads(line.removeprefix("PROXY_POOL_JSON='")[:-1])[0][
        "password"
    ] == "p:a:ss"
    with pytest.raises(module.SecretMigrationError, match="overwrite"):
        module.apply_plan(
            plan,
            expected_sha256=plan["canonical_sha256"],
            expected_plan_sha256=plan["plan_sha256"],
        )


def stat_mode(path: Path) -> int:
    return os.stat(path).st_mode & 0o777


def test_source_symlink_is_rejected(tmp_path):
    module = _load()
    source = tmp_path / "real.txt"
    source.write_text("pool.example:10000:user:password\n")
    link = tmp_path / "link.txt"
    link.symlink_to(source)

    with pytest.raises(module.SecretMigrationError, match="unavailable"):
        module.build_plan(link, tmp_path / "output")


def test_main_redacts_chained_parser_value(tmp_path, capsys):
    module = _load()
    source = tmp_path / "proxys.txt"
    sentinel = "do-not-echo-port-secret"
    source.write_text(f"pool.example:{sentinel}:user:password\n")

    assert module.main([
        "--source", str(source), "--output", str(tmp_path / "output"),
    ]) == 1
    captured = capsys.readouterr()
    assert sentinel not in captured.out
    assert sentinel not in captured.err
    assert "secret migration error:" in captured.err


def test_runtime_proxy_parser_is_mandatory(monkeypatch):
    module = _load()
    monkeypatch.setattr(module.importlib.util, "spec_from_file_location", lambda *_: None)
    with pytest.raises(
        module.SecretMigrationError,
        match="runtime proxy parser is unavailable",
    ):
        module.validate_runtime_parser("[]", ())


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker CLI unavailable")
def test_compose_single_quoted_env_round_trip(tmp_path):
    module = _load()
    password = "p:a$#\\ss'word"
    records = module.parse_legacy_pool(
        f"pool.example:10000:user:{password}\n".encode(),
    )
    canonical = module.canonical_pool_json(records)
    env_file = tmp_path / "proxy.env"
    env_file.write_text(
        f"PROXY_POOL_JSON='{canonical}'\n",
        encoding="utf-8",
    )
    compose_file = tmp_path / "compose.yaml"
    compose_file.write_text(
        "services:\n"
        "  probe:\n"
        "    image: scratch\n"
        "    environment:\n"
        "      PROXY_POOL_JSON: ${PROXY_POOL_JSON:-}\n",
        encoding="utf-8",
    )

    configured = subprocess.run(
        [
            "docker",
            "compose",
            "--project-directory",
            str(tmp_path),
            "--env-file",
            str(env_file),
            "-f",
            str(compose_file),
            "config",
            "--format",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    environment = json.loads(configured.stdout)["services"]["probe"]["environment"]
    assert json.loads(environment["PROXY_POOL_JSON"])[0]["password"] == password
