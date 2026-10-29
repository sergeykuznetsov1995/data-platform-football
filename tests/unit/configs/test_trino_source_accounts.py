"""#1937: per-source Trino accounts and memory sizing stay consistent.

Checks the committed Trino config, not a live server: every src_<source>
account that the resource-group selector routes must be allowed to write by
rules.json, and the JVM/query memory numbers must fit the compose limit.
"""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
TRINO = ROOT / "configs" / "trino"
SOURCES = ["fbref", "fotmob", "sofascore", "whoscored", "transfermarkt",
           "espn", "understat", "clubelo"]


def _selector_for(user):
    rg = json.loads((TRINO / "resource-groups.json").read_text())
    for sel in rg["selectors"]:
        if "user" in sel and re.fullmatch(sel["user"], user):
            return sel
    return None


@pytest.mark.unit
@pytest.mark.parametrize("source", SOURCES)
def test_source_user_routed_to_own_resource_group(source):
    user = f"src_{source}"
    sel = _selector_for(user)
    assert sel is not None, f"{user} has no resource-group selector"
    assert sel["group"] == "sources.${USER}"
    rg = json.loads((TRINO / "resource-groups.json").read_text())
    sources = next(g for g in rg["rootGroups"] if g["name"] == "sources")
    sub = sources["subGroups"][0]
    assert sub["name"] == "${USER}"
    assert sub["softMemoryLimit"].endswith("%") and int(sub["softMemoryLimit"][:-1]) < 100


@pytest.mark.unit
@pytest.mark.parametrize("source", SOURCES)
def test_source_user_may_write_like_airflow(source):
    user = f"src_{source}"
    rules = json.loads((TRINO / "rules.json").read_text())
    for section in ("catalogs", "schemas", "tables", "procedures"):
        first = next(r for r in rules[section] if "user" in r)
        assert re.fullmatch(first["user"], user), f"{user} not in rules.{section}"
        assert re.fullmatch(first["user"], "airflow")
    # humans and unapproved src_* names must not match the machine regex
    for stranger in ("sergey", "src_alice", "src_fbref2", "xsrc_fbref"):
        assert not re.fullmatch(rules["tables"][0]["user"], stranger), stranger
        sel = _selector_for(stranger)
        assert sel is None or sel["group"] == "humans", stranger


SCRIPT = ROOT / "scripts" / "trino" / "sync_source_users.sh"


@pytest.mark.unit
def test_env_example_lists_every_source_password():
    env_example = (ROOT / ".env.example").read_text()
    for source in SOURCES:
        assert f"TRINO_SRC_{source.upper()}_PASSWORD=" in env_example


@pytest.mark.unit
def test_sync_script_renders_eight_bcrypt_accounts(tmp_path):
    if shutil.which("htpasswd") is None:
        pytest.skip("htpasswd (apache2-utils) not installed")
    env = tmp_path / ".env"
    # the shared .env contains lines bash cannot source — the script must not
    # `. .env` (29.10: OM_AUTH_PUBLIC_KEYS=[https://...] broke the prod render)
    env.write_text("OM_AUTH_PUBLIC_KEYS=[https://a.example/certs, https://b.example/certs]\n"
                   + "".join(f"TRINO_SRC_{s.upper()}_PASSWORD=pw-{s}\n" for s in SOURCES)
                   + "TRINO_SRC_NOT_A_SOURCE_PASSWORD=ignored\n")
    db = tmp_path / "password.db"
    db.write_text("airflow:$2y$10$existinghash\n")
    db.chmod(0o600)  # a stricter mode left by an operator must be repaired
    res = subprocess.run([str(SCRIPT), str(db), str(env)], capture_output=True, text=True)
    assert res.returncode == 0, res.stderr
    lines = db.read_text().splitlines()
    assert lines[0] == "airflow:$2y$10$existinghash", "other users untouched"
    users = {l.split(":", 1)[0]: l.split(":", 1)[1] for l in lines[1:]}
    assert set(users) == {f"src_{s}" for s in SOURCES}
    assert all(h.startswith("$2y$10$") for h in users.values()), users
    # bind-mounted into the container, read by uid 1000, not root: must stay 644
    assert oct(db.stat().st_mode & 0o777) == "0o644", oct(db.stat().st_mode)
    # second run replaces, never duplicates
    res = subprocess.run([str(SCRIPT), str(db), str(env)], capture_output=True, text=True)
    assert res.returncode == 0 and len(db.read_text().splitlines()) == 9


@pytest.mark.unit
def test_sync_script_refuses_missing_or_placeholder_password(tmp_path):
    if shutil.which("htpasswd") is None:
        pytest.skip("htpasswd (apache2-utils) not installed")
    env = tmp_path / ".env"
    env.write_text("".join(f"TRINO_SRC_{s.upper()}_PASSWORD=pw-{s}\n" for s in SOURCES[:-1])
                   + f"TRINO_SRC_{SOURCES[-1].upper()}_PASSWORD=change-me\n")
    db = tmp_path / "password.db"
    res = subprocess.run([str(SCRIPT), str(db), str(env)], capture_output=True, text=True)
    assert res.returncode == 1
    assert f"TRINO_SRC_{SOURCES[-1].upper()}_PASSWORD" in res.stderr
    assert not db.exists() or db.read_text() == "", "nothing rendered on refusal"


def _gb(value):
    m = re.fullmatch(r"(\d+)\s*(G|GB|g)", value.strip())
    assert m, value
    return int(m.group(1))


@pytest.mark.unit
def test_memory_sizing_fits_compose_limit():
    compose = (ROOT / "compose.yaml").read_text()
    trino_block = compose[compose.index("\n  trino:\n"):compose.index("\n  superset:\n")]
    limit = _gb(re.search(r"limits:\s*(?:#.*\n\s*)*memory:\s*(\S+)", trino_block).group(1))
    swap = _gb(re.search(r"memswap_limit:\s*(\S+)", trino_block).group(1))
    jvm = (TRINO / "jvm.config").read_text()
    xmx = _gb(re.search(r"^-Xmx(\S+)$", jvm, re.M).group(1))
    props = dict(
        line.split("=", 1)
        for line in (TRINO / "config.properties").read_text().splitlines()
        if "=" in line and not line.startswith("#")
    )
    per_node = _gb(props["query.max-memory-per-node"])
    headroom = _gb(props["memory.heap-headroom-per-node"])
    assert swap == limit, "no swap for the JVM"
    assert xmx * 2 <= limit, "heap must leave >= 50% of the cgroup for off-heap"
    assert per_node + headroom <= xmx
    assert headroom >= 3, "planning memory of 100k-file bronze tables lives in headroom"
    assert props["query.min-expire-age"] == "10s" and props["task.info.max-age"] == "10s"
