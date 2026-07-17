from __future__ import annotations

import base64
import importlib.util
import io
import json
import os
import stat
import subprocess
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[3]
GENERATOR_PATH = ROOT / "scripts/generate_whoscored_deployment_attestation.py"
VALIDATOR_PATH = ROOT / "scripts/validate_whoscored_build_provenance.py"


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


generator = _load(GENERATOR_PATH, "generate_whoscored_deployment_attestation")
validator = _load(VALIDATOR_PATH, "deployment_attestation_validator")


def _image_fixture():
    groups = sorted(generator.IMAGE_GROUP_SERVICES)
    payload_by_group = {
        group: f"sha256:{character * 64}"
        for group, character in zip(groups, "abcdef", strict=True)
    }
    payloads = {
        service: payload_by_group[group]
        for group, services in generator.IMAGE_GROUP_SERVICES.items()
        for service in services
    }
    finals = {
        group: f"registry.invalid/whoscored/{group}@sha256:{digit * 64}"
        for group, digit in zip(groups, "123456", strict=True)
    }
    inspected = {}
    payload_config = {
        "Cmd": ["airflow", "scheduler"],
        "Entrypoint": None,
        "Env": ["SAFE=1"],
        "User": "airflow",
        "WorkingDir": "/opt/airflow",
    }
    for index, group in enumerate(groups):
        payload_id = payload_by_group[group]
        payload_layers = (f"sha256:{'abcdef'[index] * 64}",)
        inspected[payload_id] = generator.ImageInspection(
            image_id=payload_id,
            repo_digests=(),
            layers=payload_layers,
            config_raw=generator.canonical_bytes(payload_config),
        )
        if group in generator.DERIVED_FINAL_GROUPS:
            final_id = f"sha256:{str(index + 1) * 64}"
            final_layers = payload_layers + (f"sha256:{str(index + 1) * 64}",)
        else:
            final_id = payload_id
            final_layers = payload_layers
        final_config = dict(payload_config)
        if group in generator.DERIVED_FINAL_GROUPS:
            final_config.update(generator.EXPECTED_GATE_CONFIG_CHANGES)
        inspected[finals[group]] = generator.ImageInspection(
            image_id=final_id,
            repo_digests=(finals[group],),
            layers=final_layers,
            config_raw=generator.canonical_bytes(final_config),
        )
    return payloads, finals, inspected


def _provenance_fixture(finals):
    return {
        group: generator.BuildProvenanceEvidence(
            group=group,
            final_digest=finals[group],
            dockerfile_sha256="a" * 64,
            dockerfile_identity=(1,),
            metadata_sha256="b" * 64,
            metadata_identity=(2,),
            gate_inputs=(),
            source_revision="c" * 40,
            target=generator.IMAGE_GROUP_BUILD_SPECS[group][2],
        )
        for group in generator.IMAGE_GROUP_SERVICES
    }


def _buildx_metadata(
    *,
    group: str,
    final_image: str,
    revision: str,
    dockerfile_raw: bytes,
    include_gate_inputs: bool = True,
):
    _, entrypoint, target, _ = generator.IMAGE_GROUP_BUILD_SPECS[group]
    context_localdir, dockerfile_localdir = generator.IMAGE_GROUP_CONTEXT_SPECS[group]
    digest = final_image.rsplit("@", 1)[1]
    args = {"target": target} if target else {}
    if entrypoint != "Dockerfile":
        args["filename"] = entrypoint
    inputs = sorted(generator.GATE_CONTEXT_INPUTS) if include_gate_inputs else ["safe"]
    llb = [
        {
            "id": "step0",
            "op": {
                "Op": {
                    "source": {
                        "attrs": {"local.followpaths": json.dumps(inputs)},
                        "identifier": "local://context",
                    }
                }
            },
        }
    ]
    digest_mapping = {"sha256:" + "a" * 64: "step0"}
    parameters = {
        "frontend": "dockerfile.v0",
        "locals": [{"name": "context"}, {"name": "dockerfile"}],
    }
    if args:
        parameters["args"] = args
    return {
        "buildx.build.provenance": {
            "buildConfig": {
                "digestMapping": digest_mapping,
                "llbDefinition": llb,
            },
            "buildType": "https://mobyproject.org/buildkit@v1",
            "invocation": {
                "configSource": {"entryPoint": entrypoint},
                "environment": {"platform": "linux/amd64"},
                "parameters": parameters,
            },
            "metadata": {
                "completeness": {
                    "environment": True,
                    "materials": False,
                    "parameters": True,
                },
                "https://mobyproject.org/buildkit@v1#metadata": {
                    "layers": {
                        "step0:0": [
                            [
                                {
                                    "digest": "sha256:" + "b" * 64,
                                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                                    "size": 123,
                                }
                            ]
                        ]
                    },
                    "source": {
                        "infos": [
                            {
                                "data": base64.b64encode(dockerfile_raw).decode("ascii"),
                                "digestMapping": digest_mapping,
                                "filename": entrypoint,
                                "language": "Dockerfile",
                                "llbDefinition": llb,
                            }
                        ],
                        "locations": {"step0": {}},
                    },
                    "vcs": {
                        "localdir:context": context_localdir,
                        "localdir:dockerfile": dockerfile_localdir,
                        "revision": revision,
                    },
                },
                "reproducible": False,
            },
        },
        "containerimage.descriptor": {
            "digest": digest,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "size": 123,
        },
        "containerimage.digest": digest,
    }


def _gate_input_evidence():
    return tuple(
        (name, "d" * 64, (3,)) for name in sorted(generator.GATE_CONTEXT_INPUTS)
    )


def _actual_gate_input_evidence():
    context = ROOT / generator.IMAGE_GROUP_CONTEXT_SPECS["airflow-scheduler"][0]
    result = []
    for name in sorted(generator.GATE_CONTEXT_INPUTS):
        path = context / name
        metadata = path.stat(follow_symlinks=False)
        result.append(
            (
                name,
                generator.hashlib.sha256(path.read_bytes()).hexdigest(),
                tuple(
                    getattr(metadata, field)
                    for field in validator.REGULAR_FILE_IDENTITY_FIELDS
                ),
            )
        )
    return tuple(result)


def _gate_docker_archive(
    *,
    group: str = "airflow-scheduler",
    hostile: bool = False,
    extra_path: str | None = None,
    whiteout_path: str | None = None,
    root_opaque: bool = False,
    whiteout_type: str = "regular",
    malformed_whiteout: bool = False,
    duplicate_name: bool = False,
    extended_metadata_path: str | None = None,
    hardlink_path: str | None = None,
    repeat_empty_layer: bool = False,
):
    def add_whiteout(layer: tarfile.TarFile, name: str) -> None:
        member = tarfile.TarInfo(name)
        if whiteout_type == "regular":
            layer.addfile(member)
        elif whiteout_type == "nonempty":
            member.size = 1
            layer.addfile(member, io.BytesIO(b"x"))
        elif whiteout_type in {"symlink", "hardlink"}:
            member.type = (
                tarfile.SYMTYPE if whiteout_type == "symlink" else tarfile.LNKTYPE
            )
            member.linkname = "entrypoint"
            layer.addfile(member)
        elif whiteout_type == "special":
            member.type = tarfile.CHRTYPE
            member.devmajor = 1
            member.devminor = 3
            layer.addfile(member)
        else:
            raise AssertionError(whiteout_type)

    expected = {
        destination: (source.encode("utf-8"), mode)
        for destination, (source, mode) in generator.GATE_IMAGE_FILES.items()
    }
    payload_python = b"exact-payload-python"
    payload_stream = io.BytesIO()
    with tarfile.open(fileobj=payload_stream, mode="w:gz") as layer:
        payload_paths = {
            generator.INHERITED_GATE_IMAGE_FILE,
            "usr/local/bin/python3.11",
        }
        directories = set()
        for path in payload_paths:
            parts = generator.PurePosixPath(path).parts
            directories.update(
                "/".join(parts[:index]) for index in range(1, len(parts))
            )
        for directory in sorted(
            directories, key=lambda value: (value.count("/"), value)
        ):
            member = tarfile.TarInfo(directory)
            member.type = tarfile.DIRTYPE
            member.mode = 0o755
            member.uid = 0
            member.gid = 0
            layer.addfile(member)
        for path, raw, mode in (
            (
                generator.INHERITED_GATE_IMAGE_FILE,
                expected[generator.INHERITED_GATE_IMAGE_FILE][0],
                expected[generator.INHERITED_GATE_IMAGE_FILE][1],
            ),
            ("usr/local/bin/python3.11", payload_python, 0o755),
        ):
            member = tarfile.TarInfo(path)
            member.mode = mode
            member.uid = 0
            member.gid = 0
            member.size = len(raw)
            layer.addfile(member, io.BytesIO(raw))
    allowed_files, allowed_directories, _ = generator._final_suffix_policy(group)
    suffix_stream = io.BytesIO()
    with tarfile.open(fileobj=suffix_stream, mode="w:gz") as layer:
        for directory in sorted(
            allowed_directories, key=lambda value: (value.count("/"), value)
        ):
            member = tarfile.TarInfo(directory)
            member.type = tarfile.DIRTYPE
            member.mode = 0o755
            member.uid = 0
            member.gid = 0
            layer.addfile(member)
        symlinks = {
            "usr/local/bin/python": "python3",
            "usr/local/bin/python3": "python3.11",
            "opt/legacy-scraper-venv/bin/python": "python3",
            "opt/legacy-scraper-venv/bin/python3": "python3.11",
        }
        replaced_paths = {
            value.removeprefix("/")
            for value in (extended_metadata_path, hardlink_path)
            if value is not None
        }
        gate_index = 0
        for path in sorted(allowed_files):
            if path in replaced_paths:
                continue
            if path in symlinks:
                member = tarfile.TarInfo(path)
                member.type = tarfile.SYMTYPE
                member.linkname = symlinks[path]
                member.mode = 0o777
                member.uid = 0
                member.gid = 0
                layer.addfile(member)
                continue
            if path in expected:
                raw, mode = expected[path]
                if hostile and gate_index == 0:
                    raw = b"attacker"
                gate_index += 1
            elif path == "usr/local/bin/python3.11":
                raw = expected["usr/local/bin/whoscored-production-python"][0]
                mode = 0o555
            else:
                raw = payload_python
                mode = 0o555
            member = tarfile.TarInfo(path)
            member.mode = mode
            member.uid = 0
            member.gid = 0
            member.size = len(raw)
            layer.addfile(member, io.BytesIO(raw))
        if extra_path is not None:
            path = extra_path.removeprefix("/")
            raw = b"attacker"
            member = tarfile.TarInfo(path)
            member.mode = 0o555
            member.uid = 0
            member.gid = 0
            member.size = len(raw)
            layer.addfile(member, io.BytesIO(raw))
        if whiteout_path is not None:
            path = whiteout_path.removeprefix("/")
            parent, _, basename = path.rpartition("/")
            whiteout = f"{parent}/.wh.{basename}" if parent else f".wh.{basename}"
            add_whiteout(layer, whiteout)
        if root_opaque:
            add_whiteout(layer, ".wh..wh..opq")
        if malformed_whiteout:
            layer.addfile(tarfile.TarInfo(".wh."))
        if duplicate_name:
            first = tarfile.TarInfo("usr")
            first.type = tarfile.DIRTYPE
            layer.addfile(first)
            duplicate = tarfile.TarInfo("./usr")
            duplicate.type = tarfile.DIRTYPE
            layer.addfile(duplicate)
        if extended_metadata_path is not None:
            path = extended_metadata_path.removeprefix("/")
            member = tarfile.TarInfo(path)
            member.mode = 0o555
            member.uid = 0
            member.gid = 0
            member.pax_headers = {"SCHILY.xattr.security.capability": "attacker"}
            raw = b"attacker"
            member.size = len(raw)
            layer.addfile(member, io.BytesIO(raw))
        if hardlink_path is not None:
            path = hardlink_path.removeprefix("/")
            member = tarfile.TarInfo(path)
            member.type = tarfile.LNKTYPE
            member.linkname = "entrypoint"
            member.mode = 0o555
            member.uid = 0
            member.gid = 0
            layer.addfile(member)
    layer_raws = [payload_stream.getvalue()]
    payload_layer_count = 1
    if repeat_empty_layer:
        empty_stream = io.BytesIO()
        with tarfile.open(fileobj=empty_stream, mode="w:gz"):
            pass
        empty_raw = empty_stream.getvalue()
        layer_raws.append(empty_raw)
        payload_layer_count += 1
    layer_raws.append(suffix_stream.getvalue())
    if repeat_empty_layer:
        layer_raws.append(empty_raw)
    layer_names = [
        f"blobs/sha256/{generator.hashlib.sha256(raw).hexdigest()}"
        for raw in layer_raws
    ]
    manifest_raw = json.dumps(
        [{"Config": "config.json", "Layers": layer_names, "RepoTags": None}],
        separators=(",", ":"),
    ).encode("utf-8")
    outer_stream = io.BytesIO()
    with tarfile.open(fileobj=outer_stream, mode="w") as outer:
        emitted_layers = set()
        for layer_name, layer_raw in zip(layer_names, layer_raws, strict=True):
            if layer_name in emitted_layers:
                continue
            emitted_layers.add(layer_name)
            layer_member = tarfile.TarInfo(layer_name)
            layer_member.size = len(layer_raw)
            outer.addfile(layer_member, io.BytesIO(layer_raw))
        manifest_member = tarfile.TarInfo("manifest.json")
        manifest_member.size = len(manifest_raw)
        outer.addfile(manifest_member, io.BytesIO(manifest_raw))
    outer_stream.seek(0)
    return outer_stream, expected, payload_layer_count, len(layer_raws)


def test_render_expands_six_images_to_exact_fifteen_validator_records() -> None:
    payloads, finals, inspected = _image_fixture()
    manifest_digest = "9" * 64

    raw = generator.render_deployment_attestation(
        manifest_sha256=manifest_digest,
        payloads=payloads,
        final_images=finals,
        build_provenance=_provenance_fixture(finals),
        inspect_image=inspected.__getitem__,
    )
    repeated = generator.render_deployment_attestation(
        manifest_sha256=manifest_digest,
        payloads=dict(reversed(tuple(payloads.items()))),
        final_images=dict(reversed(tuple(finals.items()))),
        build_provenance=_provenance_fixture(finals),
        inspect_image=inspected.__getitem__,
    )

    document = json.loads(raw)
    assert repeated == raw
    assert raw == generator.canonical_bytes(document)
    assert set(document) == {
        "images",
        "provenance_manifest_sha256",
        "schema_version",
        "status",
    }
    assert document["status"] == "ready-v1"
    assert len(document["images"]) == 15
    assert [item["service"] for item in document["images"]] == sorted(
        generator.EXPECTED_SERVICES
    )
    assert {
        item["final_image"]
        for item in document["images"]
        if item["service"] in generator.IMAGE_GROUP_SERVICES["superset"]
    } == {finals["superset"]}


def test_ready_evidence_preserves_all_fifteen_service_bindings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads, _, _ = _image_fixture()
    discovery = SimpleNamespace(
        validated_payload_image_ids=payloads,
        validated_manifest_sha256="9" * 64,
        validated_payload_revision="7" * 40,
        validated_release_revision="8" * 40,
        build_attestation_raw=b"attestation",
        build_attestation_identity=(1,),
        build_manifest_raw=b"manifest",
        build_manifest_identity=(2,),
    )
    fake_validator = SimpleNamespace(
        ProvenanceError=RuntimeError,
        _resolve_git_dir=lambda root: root / ".git-metadata-test",
        subprocess=SimpleNamespace(),
        validate_ready_build_evidence=lambda *args, **kwargs: discovery,
    )
    monkeypatch.setattr(generator, "_validator_module", lambda: fake_validator)
    monkeypatch.setattr(
        generator,
        "_open_trusted_executable",
        lambda *args, **kwargs: os.open("/dev/null", os.O_RDONLY),
    )

    evidence = generator._validated_ready_evidence(ROOT)

    assert evidence.payloads == payloads
    assert len(evidence.payload_image_ids) == 15


def test_published_document_is_accepted_by_existing_validator(tmp_path: Path) -> None:
    payloads, finals, inspected = _image_fixture()
    manifest_digest = "9" * 64
    raw = generator.render_deployment_attestation(
        manifest_sha256=manifest_digest,
        payloads=payloads,
        final_images=finals,
        build_provenance=_provenance_fixture(finals),
        inspect_image=inspected.__getitem__,
    )
    output = tmp_path / "deployment-attestation.json"

    generator._publish_new_file(output, raw)
    generator._verify_published(output, raw)
    local_images = [
        {"service": service, "payload_image_id": payloads[service]}
        for service in sorted(payloads)
    ]
    document, observed, _, final_images = validator._validate_deployment_attestation(
        output,
        manifest_digest=manifest_digest,
        local_images=local_images,
    )

    assert observed == raw
    assert len(document["images"]) == 15
    assert final_images["airflow-scheduler"] == finals["airflow-scheduler"]
    assert stat.S_IMODE(output.stat().st_mode) == 0o600


@pytest.mark.parametrize("mutation", ["missing", "extra", "split-group"])
def test_payload_service_set_and_shared_groups_are_exact(mutation: str) -> None:
    payloads, finals, inspected = _image_fixture()
    if mutation == "missing":
        payloads.pop("airflow-init")
    elif mutation == "extra":
        payloads["attacker"] = "sha256:" + "0" * 64
    else:
        payloads["superset-worker"] = "sha256:" + "0" * 64

    with pytest.raises(generator.DeploymentAttestationError, match="fifteen|differ"):
        generator.render_deployment_attestation(
            manifest_sha256="9" * 64,
            payloads=payloads,
            final_images=finals,
            build_provenance=_provenance_fixture(finals),
            inspect_image=inspected.__getitem__,
        )


@pytest.mark.parametrize("mutation", ["digest", "ancestry", "payload-id"])
def test_docker_identity_mismatch_fails_closed(mutation: str) -> None:
    payloads, finals, inspected = _image_fixture()
    group = "airflow-scheduler"
    payload_id = payloads[group]
    if mutation == "digest":
        inspected[finals[group]] = generator.ImageInspection(
            image_id=inspected[finals[group]].image_id,
            repo_digests=(),
            layers=inspected[finals[group]].layers,
            config_raw=inspected[finals[group]].config_raw,
        )
    elif mutation == "ancestry":
        inspected[finals[group]] = generator.ImageInspection(
            image_id=inspected[finals[group]].image_id,
            repo_digests=(finals[group],),
            layers=("sha256:" + "0" * 64, "sha256:" + "1" * 64),
            config_raw=inspected[finals[group]].config_raw,
        )
    else:
        inspected[payload_id] = generator.ImageInspection(
            image_id="sha256:" + "0" * 64,
            repo_digests=(),
            layers=inspected[payload_id].layers,
            config_raw=inspected[payload_id].config_raw,
        )

    with pytest.raises(generator.DeploymentAttestationError, match="Docker|digest|descendant"):
        generator.render_deployment_attestation(
            manifest_sha256="9" * 64,
            payloads=payloads,
            final_images=finals,
            build_provenance=_provenance_fixture(finals),
            inspect_image=inspected.__getitem__,
        )


def test_hostile_descendant_cannot_replace_gate_config() -> None:
    payloads, finals, inspected = _image_fixture()
    group = "airflow-scheduler"
    hostile = json.loads(inspected[finals[group]].config_raw)
    hostile["Entrypoint"] = ["/attacker"]
    inspected[finals[group]] = generator.ImageInspection(
        image_id=inspected[finals[group]].image_id,
        repo_digests=inspected[finals[group]].repo_digests,
        layers=inspected[finals[group]].layers,
        config_raw=generator.canonical_bytes(hostile),
    )

    with pytest.raises(generator.DeploymentAttestationError, match="config"):
        generator.render_deployment_attestation(
            manifest_sha256="9" * 64,
            payloads=payloads,
            final_images=finals,
            build_provenance=_provenance_fixture(finals),
            inspect_image=inspected.__getitem__,
        )


def test_final_image_archive_binds_exact_gate_file_bytes_and_metadata() -> None:
    archive, expected, payload_layers, final_layers = _gate_docker_archive()

    generator._verify_gate_archive(
        archive,
        group="airflow-scheduler",
        payload_layer_count=payload_layers,
        final_layer_count=final_layers,
        expected_files=expected,
    )


def test_repeated_identical_empty_buildkit_layers_are_valid() -> None:
    archive, expected, payload_layers, final_layers = _gate_docker_archive(
        repeat_empty_layer=True
    )

    generator._verify_gate_archive(
        archive,
        group="airflow-scheduler",
        payload_layer_count=payload_layers,
        final_layer_count=final_layers,
        expected_files=expected,
    )


def test_hostile_descendant_cannot_replace_gate_file_bytes() -> None:
    archive, expected, payload_layers, final_layers = _gate_docker_archive(hostile=True)

    with pytest.raises(
        generator.DeploymentAttestationError, match="suffix file content|gate file differs"
    ):
        generator._verify_gate_archive(
            archive,
            group="airflow-scheduler",
            payload_layer_count=payload_layers,
            final_layer_count=final_layers,
            expected_files=expected,
        )


@pytest.mark.parametrize(
    "path",
    [
        "/usr/bin/dumb-init",
        "/entrypoint",
        "/usr/local/lib/python3.11/site-packages/attacker.py",
    ],
)
def test_final_suffix_rejects_every_unreviewed_changed_path(path: str) -> None:
    archive, expected, payload_layers, final_layers = _gate_docker_archive(
        extra_path=path
    )

    with pytest.raises(generator.DeploymentAttestationError, match="unreviewed path"):
        generator._verify_gate_archive(
            archive,
            group="airflow-scheduler",
            payload_layer_count=payload_layers,
            final_layer_count=final_layers,
            expected_files=expected,
        )


@pytest.mark.parametrize(
    "path",
    [
        "/usr/bin/dumb-init",
        "/entrypoint",
        "/usr/local/lib/python3.11/site-packages/attacker.py",
    ],
)
def test_final_suffix_rejects_every_unreviewed_whiteout(path: str) -> None:
    archive, expected, payload_layers, final_layers = _gate_docker_archive(
        whiteout_path=path
    )

    with pytest.raises(generator.DeploymentAttestationError, match="removes an unreviewed"):
        generator._verify_gate_archive(
            archive,
            group="airflow-scheduler",
            payload_layer_count=payload_layers,
            final_layer_count=final_layers,
            expected_files=expected,
        )


@pytest.mark.parametrize(
    ("whiteout_type", "root_opaque"),
    [
        ("symlink", False),
        ("hardlink", False),
        ("nonempty", False),
        ("special", False),
        ("symlink", True),
    ],
)
def test_whiteout_must_be_an_empty_regular_file(
    whiteout_type: str, root_opaque: bool
) -> None:
    archive, expected, payload_layers, final_layers = _gate_docker_archive(
        whiteout_path=None if root_opaque else "/usr/local/bin/python",
        root_opaque=root_opaque,
        whiteout_type=whiteout_type,
    )

    with pytest.raises(generator.DeploymentAttestationError, match="malformed whiteout"):
        generator._verify_gate_archive(
            archive,
            group="airflow-scheduler",
            payload_layer_count=payload_layers,
            final_layer_count=final_layers,
            expected_files=expected,
        )


@pytest.mark.parametrize(
    ("mutation", "path", "error"),
    [
        (
            "extended",
            "/usr/local/bin/python3.11",
            "extended archive metadata",
        ),
        (
            "hardlink",
            "/usr/local/libexec/whoscored-python-real",
            "file type or owner",
        ),
    ],
)
def test_final_suffix_rejects_extended_metadata_and_hardlinks(
    mutation: str, path: str, error: str
) -> None:
    archive, expected, payload_layers, final_layers = _gate_docker_archive(
        extended_metadata_path=path if mutation == "extended" else None,
        hardlink_path=path if mutation == "hardlink" else None,
    )

    with pytest.raises(generator.DeploymentAttestationError, match=error):
        generator._verify_gate_archive(
            archive,
            group="airflow-scheduler",
            payload_layer_count=payload_layers,
            final_layer_count=final_layers,
            expected_files=expected,
        )


def test_root_opaque_whiteout_removes_all_lower_gate_files() -> None:
    archive, expected, payload_layers, final_layers = _gate_docker_archive(
        root_opaque=True
    )

    with pytest.raises(generator.DeploymentAttestationError, match="opaque-directory"):
        generator._verify_gate_archive(
            archive,
            group="airflow-scheduler",
            payload_layer_count=payload_layers,
            final_layer_count=final_layers,
            expected_files=expected,
        )


@pytest.mark.parametrize("mutation", ["malformed-whiteout", "duplicate-name"])
def test_ambiguous_layer_member_names_fail_closed(mutation: str) -> None:
    archive, expected, payload_layers, final_layers = _gate_docker_archive(
        malformed_whiteout=mutation == "malformed-whiteout",
        duplicate_name=mutation == "duplicate-name",
    )

    with pytest.raises(
        generator.DeploymentAttestationError, match="whiteout|duplicate"
    ):
        generator._verify_gate_archive(
            archive,
            group="airflow-scheduler",
            payload_layer_count=payload_layers,
            final_layer_count=final_layers,
            expected_files=expected,
        )


def test_buildkit_max_provenance_binds_digest_target_dockerfile_and_gate() -> None:
    _, finals, _ = _image_fixture()
    group = "airflow-scheduler"
    dockerfile_raw = (ROOT / "docker/images/airflow/Dockerfile").read_bytes()
    revision = "8" * 40
    document = _buildx_metadata(
        group=group,
        final_image=finals[group],
        revision=revision,
        dockerfile_raw=dockerfile_raw,
    )

    evidence = generator._validate_build_provenance_value(
        group=group,
        final_image=finals[group],
        expected_revision=revision,
        raw=generator.canonical_bytes(document),
        identity=(1,),
        dockerfile_raw=dockerfile_raw,
        dockerfile_identity=(2,),
        gate_inputs=_gate_input_evidence(),
    )

    assert evidence.final_digest == finals[group]
    assert evidence.target == "airflow-scheduler"
    assert evidence.dockerfile_sha256 == generator.hashlib.sha256(
        dockerfile_raw
    ).hexdigest()


@pytest.mark.parametrize(
    "mutation",
    ["digest", "target", "dockerfile", "gate-input", "dirty-revision"],
)
def test_buildkit_provenance_rejects_hostile_final_build(mutation: str) -> None:
    _, finals, _ = _image_fixture()
    group = "airflow-scheduler"
    dockerfile_raw = (ROOT / "docker/images/airflow/Dockerfile").read_bytes()
    revision = "8" * 40
    document = _buildx_metadata(
        group=group,
        final_image=finals[group],
        revision=revision,
        dockerfile_raw=dockerfile_raw,
        include_gate_inputs=mutation != "gate-input",
    )
    if mutation == "digest":
        document["containerimage.digest"] = "sha256:" + "0" * 64
    elif mutation == "target":
        document["buildx.build.provenance"]["invocation"]["parameters"]["args"][
            "target"
        ] = "attacker"
    elif mutation == "dockerfile":
        document["buildx.build.provenance"]["metadata"][
            "https://mobyproject.org/buildkit@v1#metadata"
        ]["source"]["infos"][0]["data"] = base64.b64encode(b"FROM scratch\n").decode(
            "ascii"
        )
    elif mutation == "dirty-revision":
        document["buildx.build.provenance"]["metadata"][
            "https://mobyproject.org/buildkit@v1#metadata"
        ]["vcs"]["revision"] = revision + "-dirty"

    with pytest.raises(generator.DeploymentAttestationError, match="BuildKit"):
        generator._validate_build_provenance_value(
            group=group,
            final_image=finals[group],
            expected_revision=revision,
            raw=generator.canonical_bytes(document),
            identity=(1,),
            dockerfile_raw=dockerfile_raw,
            dockerfile_identity=(2,),
            gate_inputs=_gate_input_evidence(),
        )


def test_pinned_git_ignores_inherited_git_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {}

    def fake_run(command, **kwargs):
        observed["command"] = command
        observed["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0, stdout=b"ok")

    monkeypatch.setenv("GIT_DIR", "/attacker")
    monkeypatch.setenv("PATH", "/attacker")
    monkeypatch.setattr(generator.subprocess, "run", fake_run)

    result = generator._PinnedGitSubprocess(17).run(
        ("git", "-C", "/release", "rev-parse", "HEAD"),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )

    assert result.returncode == 0
    command = observed["command"]
    assert command[0] == "/proc/self/fd/17"
    assert "core.fsmonitor=false" in command
    assert "core.hooksPath=/dev/null" in command
    assert "filter.lfs.process=" in command
    assert command[-4:] == ("-C", "/release", "rev-parse", "HEAD")
    assert observed["kwargs"]["pass_fds"] == (17,)
    assert observed["kwargs"]["env"] == {
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_EXTERNAL_DIFF": "/bin/false",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_PAGER": "/bin/false",
        "GIT_TERMINAL_PROMPT": "0",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }


def test_pinned_git_disables_hostile_local_fsmonitor(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    environment = {
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_NOSYSTEM": "1",
        "HOME": "/nonexistent",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin",
    }
    subprocess.run(("/usr/bin/git", "init", str(repo)), check=True, env=environment)
    sentinel = tmp_path / "local-config-executed"
    hook = f"sh -c 'touch {sentinel}; exit 0'"
    subprocess.run(
        ("/usr/bin/git", "-C", str(repo), "config", "core.fsmonitor", hook),
        check=True,
        env=environment,
    )
    subprocess.run(
        ("/usr/bin/git", "-C", str(repo), "status", "--porcelain"),
        check=True,
        env=environment,
        stdout=subprocess.PIPE,
    )
    assert sentinel.exists()
    sentinel.unlink()

    git_fd = os.open("/usr/bin/git", os.O_RDONLY)
    try:
        generator._PinnedGitSubprocess(git_fd).run(
            ("git", "-C", str(repo), "status", "--porcelain"),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    finally:
        os.close(git_fd)
    assert not sentinel.exists()


def test_git_attributes_that_can_select_filters_are_rejected(tmp_path: Path) -> None:
    (tmp_path / ".gitattributes").write_text(
        "* filter=attacker diff=attacker\n", encoding="utf-8"
    )

    with pytest.raises(generator.DeploymentAttestationError, match="attributes"):
        generator._require_no_git_attributes(tmp_path)


def test_final_bindings_reject_missing_duplicate_and_mutable_references() -> None:
    _, finals, _ = _image_fixture()
    missing = dict(finals)
    missing.pop("superset")
    with pytest.raises(generator.DeploymentAttestationError, match="six groups"):
        generator._validate_final_images(missing)

    duplicate = dict(finals)
    duplicate["superset"] = duplicate["jupyterhub"]
    with pytest.raises(generator.DeploymentAttestationError, match="distinct"):
        generator._validate_final_images(duplicate)

    mutable = dict(finals)
    mutable["superset"] = "registry.invalid/whoscored/superset:latest"
    with pytest.raises(generator.DeploymentAttestationError, match="repository@"):
        generator._validate_final_images(mutable)


def test_six_registry_digests_must_resolve_to_six_local_image_ids() -> None:
    payloads, finals, inspected = _image_fixture()
    scheduler = inspected[finals["airflow-scheduler"]]
    inspected[finals["airflow-scheduler"]] = generator.ImageInspection(
        image_id=payloads["airflow-init"],
        repo_digests=scheduler.repo_digests,
        layers=scheduler.layers,
        config_raw=scheduler.config_raw,
    )

    with pytest.raises(generator.DeploymentAttestationError, match="duplicate image IDs"):
        generator.render_deployment_attestation(
            manifest_sha256="9" * 64,
            payloads=payloads,
            final_images=finals,
            build_provenance=_provenance_fixture(finals),
            inspect_image=inspected.__getitem__,
        )


def test_inspect_parser_accepts_only_one_bounded_canonical_identity_line() -> None:
    reference = "registry.invalid/whoscored/image@sha256:" + "1" * 64
    raw = generator.canonical_bytes("sha256:" + "2" * 64)[:-1]
    raw += b"\t" + generator.canonical_bytes([reference])[:-1]
    raw += b"\t" + generator.canonical_bytes(
        {"Layers": ["sha256:" + "3" * 64], "Type": "layers"}
    )[:-1]
    raw += b"\t" + generator.canonical_bytes(
        {"Entrypoint": ["/bin/true"], "User": "50000:0"}
    )

    inspection = generator._parse_inspection(raw, reference=reference)

    assert inspection.image_id == "sha256:" + "2" * 64
    assert inspection.repo_digests == (reference,)
    with pytest.raises(generator.DeploymentAttestationError, match="malformed"):
        generator._parse_inspection(raw + b"extra\n", reference=reference)


def test_real_buildx_metadata_and_docker_digest_path_when_ci_provides_it() -> None:
    if os.environ.get("WHOSCORED_REAL_DOCKER_TEST") != "1":
        pytest.skip("real Docker evidence is built only by the WhoScored CI job")
    metadata_path = Path(os.environ["WHOSCORED_SCHEDULER_BUILD_METADATA"])
    image_tag = os.environ["WHOSCORED_SCHEDULER_IMAGE"]
    payload_image_tag = os.environ["WHOSCORED_SCHEDULER_PAYLOAD_IMAGE"]
    raw = metadata_path.read_bytes()
    document = json.loads(raw)
    digest = document["containerimage.digest"]
    repository = image_tag.rsplit(":", 1)[0]
    final_image = f"{repository}@{digest}"
    revision = subprocess.run(
        ("/usr/bin/git", "-C", str(ROOT), "rev-parse", "HEAD"),
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.strip()
    dockerfile_raw = (ROOT / "docker/images/airflow/Dockerfile").read_bytes()
    metadata_stat = metadata_path.stat(follow_symlinks=False)
    identity = tuple(
        getattr(metadata_stat, field) for field in validator.REGULAR_FILE_IDENTITY_FIELDS
    )

    evidence = generator._validate_build_provenance_value(
        group="airflow-scheduler",
        final_image=final_image,
        expected_revision=revision,
        raw=raw,
        identity=identity,
        dockerfile_raw=dockerfile_raw,
        dockerfile_identity=(1,),
        gate_inputs=_actual_gate_input_evidence(),
    )
    docker_fd = generator._open_trusted_docker()
    try:
        observed = subprocess.run(
            (
                f"/proc/self/fd/{docker_fd}",
                "image",
                "inspect",
                "--format",
                "{{.Id}}",
                "--",
                image_tag,
            ),
            check=True,
            env={
                "DOCKER_CONFIG": "/nonexistent",
                "DOCKER_HOST": "unix:///run/docker.sock",
                "HOME": "/nonexistent",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PATH": "/usr/bin:/bin",
            },
            pass_fds=(docker_fd,),
            stdout=subprocess.PIPE,
            text=True,
        )
        image_id = observed.stdout.strip()
        inspection = generator._inspect_with_docker(image_id, docker_fd=docker_fd)
        payload_observed = subprocess.run(
            (
                f"/proc/self/fd/{docker_fd}",
                "image",
                "inspect",
                "--format",
                "{{.Id}}",
                "--",
                payload_image_tag,
            ),
            check=True,
            env={
                "DOCKER_CONFIG": "/nonexistent",
                "DOCKER_HOST": "unix:///run/docker.sock",
                "HOME": "/nonexistent",
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PATH": "/usr/bin:/bin",
            },
            pass_fds=(docker_fd,),
            stdout=subprocess.PIPE,
            text=True,
        )
        payload_inspection = generator._inspect_with_docker(
            payload_observed.stdout.strip(), docker_fd=docker_fd
        )
        archive_reference = image_id
        if final_image in inspection.repo_digests:
            inspection = generator._inspect_with_docker(
                final_image, docker_fd=docker_fd
            )
            archive_reference = final_image
        context = ROOT / generator.IMAGE_GROUP_CONTEXT_SPECS["airflow-scheduler"][0]
        expected_gate_files = {
            destination: ((context / source).read_bytes(), mode)
            for destination, (source, mode) in generator.GATE_IMAGE_FILES.items()
        }
        generator._verify_final_gate_files(
            archive_reference,
            group="airflow-scheduler",
            payload_layer_count=len(payload_inspection.layers),
            final_layer_count=len(inspection.layers),
            docker_fd=docker_fd,
            expected_files=expected_gate_files,
        )
    finally:
        os.close(docker_fd)

    assert evidence.final_digest == final_image
    assert inspection.image_id == image_id
    assert json.loads(inspection.config_raw)["Entrypoint"] == list(
        generator.EXPECTED_GATE_CONFIG_CHANGES["Entrypoint"]
    )


def test_publish_is_create_once_and_never_follows_existing_symlink(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target.json"
    target.write_text("keep", encoding="utf-8")
    output = tmp_path / "deployment.json"
    output.symlink_to(target)

    with pytest.raises(generator.DeploymentAttestationError, match="already exists"):
        generator._publish_new_file(output, b"{}\n")

    assert target.read_text(encoding="utf-8") == "keep"


def test_cli_binding_parser_rejects_duplicates() -> None:
    _, finals, _ = _image_fixture()
    values = [f"{group}={reference}" for group, reference in finals.items()]
    values.append(values[0])

    with pytest.raises(generator.DeploymentAttestationError, match="duplicated"):
        generator._final_image_mapping(values)


def test_cli_build_metadata_parser_requires_six_distinct_absolute_paths() -> None:
    values = [
        f"{group}=/protected/{group}.json"
        for group in generator.IMAGE_GROUP_SERVICES
    ]
    assert set(generator._build_metadata_mapping(values)) == set(
        generator.IMAGE_GROUP_SERVICES
    )

    values[0] = values[0].split("=", 1)[0] + "=relative.json"
    with pytest.raises(generator.DeploymentAttestationError, match="invalid"):
        generator._build_metadata_mapping(values)


def test_build_metadata_loader_requires_root_mode_0600(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = b'{"metadata":true}\n'
    fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_nlink",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )

    def identity(mode: int):
        return (1, 2, stat.S_IFREG | mode, 0, 0, 1, len(raw), 3, 4)

    fake_validator = SimpleNamespace(
        ProvenanceError=RuntimeError,
        REGULAR_FILE_IDENTITY_FIELDS=fields,
        read_protected_regular_file_snapshot=lambda *args, **kwargs: (
            raw,
            identity(0o600),
        ),
    )
    monkeypatch.setattr(generator, "_validator_module", lambda: fake_validator)

    observed, _ = generator._load_protected_build_metadata(Path("/evidence.json"))
    assert observed == raw

    fake_validator.read_protected_regular_file_snapshot = lambda *args, **kwargs: (
        raw,
        identity(0o644),
    )
    with pytest.raises(generator.DeploymentAttestationError, match="0600"):
        generator._load_protected_build_metadata(Path("/evidence.json"))


def test_cli_runtime_rejects_an_isolated_non_system_interpreter() -> None:
    assert sys.executable != "/usr/bin/python3"
    with pytest.raises(generator.DeploymentAttestationError, match="system Python"):
        generator._require_isolated_process()


def test_cli_runtime_rejects_inherited_git_controls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_sys = SimpleNamespace(
        executable="/usr/bin/python3",
        flags=SimpleNamespace(isolated=1, no_site=1, ignore_environment=1),
    )
    monkeypatch.setattr(generator, "sys", fake_sys)
    monkeypatch.setattr(generator.os, "geteuid", lambda: 0)
    for name in tuple(os.environ):
        if name in generator._CONTROL_ENV_NAMES or any(
            name.startswith(prefix) for prefix in generator._CONTROL_ENV_PREFIXES
        ):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setenv("GIT_DIR", "/attacker")

    with pytest.raises(generator.DeploymentAttestationError, match="GIT_DIR"):
        generator._require_isolated_process()


def _ready_evidence(payloads, *, digest: str = "9" * 64):
    return generator.ReadyEvidence(
        manifest_sha256=digest,
        payload_image_ids=tuple(sorted(payloads.items())),
        payload_revision="7" * 40,
        release_revision="8" * 40,
        build_attestation_raw=b"attestation",
        build_attestation_identity=(1,),
        build_manifest_raw=b"manifest",
        build_manifest_identity=(2,),
    )


def test_generate_validates_twice_before_create_once_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payloads, finals, inspected = _image_fixture()
    evidence = _ready_evidence(payloads)
    provenance = _provenance_fixture(finals)
    metadata = {
        group: tmp_path / f"{group}.json"
        for group in generator.IMAGE_GROUP_SERVICES
    }
    evidence_calls = []
    inspect_calls = []
    monkeypatch.setattr(generator, "_require_protected_root", lambda root: root)
    monkeypatch.setattr(
        generator,
        "_validated_ready_evidence",
        lambda root: evidence_calls.append(root) or evidence,
    )
    monkeypatch.setattr(
        generator,
        "_load_all_build_provenance",
        lambda *args, **kwargs: provenance,
    )
    monkeypatch.setattr(
        generator,
        "_open_trusted_docker",
        lambda: os.open("/dev/null", os.O_RDONLY),
    )
    monkeypatch.setattr(
        generator,
        "_inspect_with_docker",
        lambda reference, *, docker_fd: inspect_calls.append(reference)
        or inspected[reference],
    )
    monkeypatch.setattr(
        generator, "_protected_gate_file_expectations", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(
        generator, "_verify_final_gate_files", lambda *args, **kwargs: None
    )
    output = tmp_path / "deployment.json"

    receipt = generator.generate_deployment_attestation(
        ROOT,
        output=output,
        final_images=finals,
        build_metadata=metadata,
    )

    assert evidence_calls == [ROOT, ROOT]
    assert len(inspect_calls) == 24
    assert receipt["service_count"] == 15
    assert receipt["image_group_count"] == 6
    assert output.exists()
    with pytest.raises(generator.DeploymentAttestationError, match="already exists"):
        generator.generate_deployment_attestation(
            ROOT,
            output=output,
            final_images=finals,
            build_metadata=metadata,
        )


def test_generate_publishes_nothing_when_ready_evidence_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payloads, finals, inspected = _image_fixture()
    provenance = _provenance_fixture(finals)
    metadata = {
        group: tmp_path / f"{group}.json"
        for group in generator.IMAGE_GROUP_SERVICES
    }
    evidence = iter(
        (
            _ready_evidence(payloads, digest="8" * 64),
            _ready_evidence(payloads, digest="9" * 64),
        )
    )
    monkeypatch.setattr(generator, "_require_protected_root", lambda root: root)
    monkeypatch.setattr(
        generator, "_validated_ready_evidence", lambda root: next(evidence)
    )
    monkeypatch.setattr(
        generator,
        "_load_all_build_provenance",
        lambda *args, **kwargs: provenance,
    )
    monkeypatch.setattr(
        generator,
        "_open_trusted_docker",
        lambda: os.open("/dev/null", os.O_RDONLY),
    )
    monkeypatch.setattr(
        generator,
        "_inspect_with_docker",
        lambda reference, *, docker_fd: inspected[reference],
    )
    monkeypatch.setattr(
        generator, "_protected_gate_file_expectations", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(
        generator, "_verify_final_gate_files", lambda *args, **kwargs: None
    )
    output = tmp_path / "deployment.json"

    with pytest.raises(generator.DeploymentAttestationError, match="changed"):
        generator.generate_deployment_attestation(
            ROOT,
            output=output,
            final_images=finals,
            build_metadata=metadata,
        )

    assert not output.exists()


def test_generate_refuses_to_dirty_the_release_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, finals, _ = _image_fixture()
    metadata = {
        group: Path("/tmp") / f"{group}.json"
        for group in generator.IMAGE_GROUP_SERVICES
    }
    monkeypatch.setattr(generator, "_require_protected_root", lambda root: root)

    with pytest.raises(generator.DeploymentAttestationError, match="outside"):
        generator.generate_deployment_attestation(
            ROOT,
            output=ROOT / "deployment-attestation.json",
            final_images=finals,
            build_metadata=metadata,
        )
