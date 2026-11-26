import gzip
import hashlib
import json
import socket
from pathlib import Path

import pytest

from dags.scripts import run_transfermarkt_core as cli
from dags.utils.transfermarkt_core_rehearsal import run_rehearsal


def _save_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _package(root, *, partial=False, status=200, nested=False):
    location = root / "example"
    players = ["7", "9"] if partial else ["7"]
    url = "https://tmapi.transfermarkt.technology/players?" + "&".join(
        f"ids[]={n}" for n in players
    )
    entries = [
        {
            "id": "7",
            "attributes": {"consultantAgencyId": 55},
            "clubAssignments": [{"clubId": 11, "type": "current"}],
        }
    ]
    if partial:
        entries.append(
            {
                "id": "9",
                "attributes": {},
                "clubAssignments": [{"clubId": 99, "type": "current"}],
            }
        )
    body = json.dumps({"success": True, "data": entries}).encode()
    digest = hashlib.sha256(body).hexdigest()
    capture_id = hashlib.sha256(url.encode() + body).hexdigest()
    record = {
        "endpoint": "representation",
        "capture_id": capture_id,
        "content_hash": digest,
        "url": url,
        "scope_id": "fixture-scope",
        "cycle_id": "fixture-cycle",
        "status_code": status,
        "fetched_at": "2026-01-01T10:00:00+00:00",
    }
    capture = location / "captures" / "representation" / f"{capture_id}.json"
    _save_json(capture, record)
    blob_name = (
        f"blobs/sha256/{digest[:2]}/{digest}.body.gz" if nested else "page.json.gz"
    )
    blob = location / blob_name
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(gzip.compress(body))
    _save_json(
        location / "manifest.json",
        {
            "fixtures": [
                {
                    "endpoint": "representation",
                    "capture_id": capture_id,
                    "blob_file": blob_name,
                }
            ]
        },
    )
    _save_json(location / "plan.json", {"representation_player_ids": players})
    return location, capture, blob


@pytest.mark.parametrize("nested", [False, True])
def test_invokes_strict_offline_cli_on_private_package(tmp_path, monkeypatch, nested):
    location, capture, blob = _package(tmp_path, nested=nested)
    captured = {}
    original = cli.main

    def inspect(argv):
        assert argv[0] == "offline"
        assert "--require-complete" in argv and "--write" not in argv
        snapshot = Path(argv[argv.index("--fixtures-root") + 1])
        report_path = Path(argv[argv.index("--report-out") + 1])
        assert snapshot != location and snapshot not in report_path.parents
        assert report_path.parent.stat().st_mode & 0o077 == 0
        # Race after snapshot selection: source changes must not affect the CLI's reads.
        (location / "plan.json").write_text("invalid")
        capture.write_text("invalid")
        blob.write_bytes(b"invalid")
        captured["snapshot"] = snapshot
        return original(argv)

    monkeypatch.setattr(cli, "main", inspect)
    monkeypatch.setattr(
        cli, "_write_options", lambda *_: pytest.fail("attempted a warehouse write")
    )
    monkeypatch.setattr(
        socket.socket,
        "connect",
        lambda *_: pytest.fail("attempted a network connection"),
    )
    result = run_rehearsal(tmp_path, "example", "GB1", "2526")
    assert result["outcomes"] == result["pages"] == result["rows"] == 1
    assert result["entities"] == {"player_representation_observations": 1}
    assert not captured["snapshot"].exists()


@pytest.mark.parametrize(("partial", "status"), [(True, 200), (False, 403)])
def test_quarantines_incomplete_replay(tmp_path, partial, status):
    _package(tmp_path, partial=partial, status=status)
    with pytest.raises(RuntimeError, match="incomplete"):
        run_rehearsal(tmp_path, "example", "GB1", "2526")


@pytest.mark.parametrize(
    ("package", "league", "season"),
    [
        ("../foreign", "GB1", "2526"),
        ("../example/.", "GB1", "2526"),
        ("/tmp", "GB1", "2526"),
        (".hidden", "GB1", "2526"),
        ("--live", "GB1", "2526"),
        ("example", "--write", "2526"),
        ("example", "GB1", "25/26"),
        (None, "GB1", "2526"),
        ("example", "GB1", 2526),
    ],
)
def test_only_accepts_simple_params(tmp_path, monkeypatch, package, league, season):
    monkeypatch.setattr(
        cli, "main", lambda *_: pytest.fail("unsafe input reached the CLI")
    )
    with pytest.raises(ValueError):
        run_rehearsal(tmp_path, package, league, season)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("endpoint", "../elsewhere"),
        ("capture_id", "../etc/passwd"),
        ("capture_id", "/a"),
        ("blob_file", "../forbidden.gz"),
        ("blob_file", "/tmp/forbidden.gz"),
        ("blob_file", "blobs/../forbidden.gz"),
        ("blob_file", r"..\forbidden.gz"),
        ("capture_file", "../different.json"),
    ],
)
def test_confines_every_manifest_reference(tmp_path, monkeypatch, field, value):
    location, _, _ = _package(tmp_path)
    manifest = json.loads((location / "manifest.json").read_text())
    manifest["fixtures"][0][field] = value
    _save_json(location / "manifest.json", manifest)
    monkeypatch.setattr(
        cli, "main", lambda *_: pytest.fail("unsafe input reached the CLI")
    )
    with pytest.raises(ValueError):
        run_rehearsal(tmp_path, "example", "GB1", "2526")


@pytest.mark.parametrize(
    "member",
    [
        "root",
        "package",
        "manifest",
        "plan",
        "captures",
        "endpoint",
        "capture",
        "blobs",
        "blob",
    ],
)
def test_rejects_symlink_for_every_input_member(tmp_path, monkeypatch, member):
    root = tmp_path / "roots"
    location, capture, blob = _package(root, nested=True)
    path = {
        "root": root,
        "package": location,
        "manifest": location / "manifest.json",
        "plan": location / "plan.json",
        "captures": location / "captures",
        "endpoint": capture.parent,
        "capture": capture,
        "blobs": location / "blobs",
        "blob": blob,
    }[member]
    target = tmp_path / "moved"
    path.rename(target)
    path.symlink_to(target)
    monkeypatch.setattr(cli, "main", lambda *_: pytest.fail("symlink reached the CLI"))
    with pytest.raises((OSError, ValueError)):
        run_rehearsal(root, "example", "GB1", "2526")
