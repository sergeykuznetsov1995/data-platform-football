"""Image-baked startup anchor for WhoScored runtime attestation.

This file is executed by a root-owned ``.pth`` file before application paths
can import any module.  It intentionally contains no imports: only builtins
and the interpreter's already-loaded built-in/frozen bootstrap modules are
used until the fd-read attestor has verified the complete release lock.
"""

_STARTUP_SYS = globals()["sys"]
_STARTUP_ROOT = str(globals().get("_WHOSCORED_RUNTIME_ROOT", "/opt/airflow"))
_STARTUP_REQUIRE_FULL = bool(
    globals().get("_WHOSCORED_REQUIRE_FULL_ATTESTATION", True)
)
_STARTUP_TRUST_ROOT = str(
    globals().get(
        "_WHOSCORED_TRUST_ROOT_PATH",
        "/usr/local/share/whoscored/runtime-trust-root",
    )
)
_STARTUP_ENFORCE_TRUST_OWNERSHIP = bool(
    globals().get("_WHOSCORED_ENFORCE_TRUST_OWNERSHIP", True)
)
_STARTUP_PRODUCTION_GATE = str(
    globals().get(
        "_WHOSCORED_PRODUCTION_GATE_PATH",
        "/usr/local/bin/whoscored-production-gate",
    )
)
if (
    _STARTUP_ENFORCE_TRUST_OWNERSHIP
    and _STARTUP_PRODUCTION_GATE != "/usr/local/bin/whoscored-production-gate"
):
    raise RuntimeError("WhoScored production gate path cannot be overridden")
_STARTUP_SITE_PHASE = str(globals().get("_WHOSCORED_SITE_PHASE", "direct"))
if _STARTUP_SITE_PHASE not in ("bootstrap", "finalize", "direct"):
    raise RuntimeError("invalid WhoScored site-startup phase")
_STARTUP_PRIVATE = {
    "attestor": None,
    "canonical": None,
    "runtime_class": None,
}

# Python imports these two optional hooks after processing ``.pth`` files but
# before the requested entrypoint.  Pre-seeding inert modules prevents a
# bind-mounted application path from executing unverified customization code.
for _customization_name in ("sitecustomize", "usercustomize"):
    if _customization_name not in _STARTUP_SYS.modules:
        _STARTUP_SYS.modules[_customization_name] = type(_STARTUP_SYS)(
            _customization_name
        )


def _validated_image_site_directory(path):
    if not (_STARTUP_REQUIRE_FULL and _STARTUP_ENFORCE_TRUST_OWNERSHIP):
        return path
    posix = _STARTUP_SYS.modules["posix"]
    flags = (
        posix.O_RDONLY
        | getattr(posix, "O_CLOEXEC", 0)
        | getattr(posix, "O_DIRECTORY", 0)
        | getattr(posix, "O_NOFOLLOW", 0)
    )
    descriptors = []
    try:
        descriptor = posix.open("/", flags)
        descriptors.append(descriptor)
        root_metadata = posix.fstat(descriptor)
        if (
            root_metadata.st_uid != 0
            or root_metadata.st_mode & 0o022
        ):
            raise RuntimeError("WhoScored image filesystem root is mutable")
        for part in tuple(item for item in path.split("/") if item):
            descriptor = posix.open(part, flags, dir_fd=descriptor)
            descriptors.append(descriptor)
            metadata = posix.fstat(descriptor)
            if (
                metadata.st_mode & 0o170000 != 0o040000
                or metadata.st_uid != 0
                or metadata.st_mode & 0o022
            ):
                raise RuntimeError(
                    "WhoScored image Python site directory is mutable: " + path
                )
    finally:
        for open_descriptor in reversed(descriptors):
            posix.close(open_descriptor)
    return path


def _trusted_python_paths(root, include_application=True):
    version = (
        str(_STARTUP_SYS.version_info.major)
        + "."
        + str(_STARTUP_SYS.version_info.minor)
    )
    library_roots = tuple(
        dict.fromkeys(
            str(prefix).rstrip("/") + "/lib/python" + version
            for prefix in (
                _STARTUP_SYS.base_prefix,
                _STARTUP_SYS.prefix,
                _STARTUP_SYS.exec_prefix,
                "/home/airflow/.local",
            )
            if prefix
        )
    )
    zip_paths = tuple(
        str(prefix).rstrip("/")
        + "/lib/python"
        + str(_STARTUP_SYS.version_info.major)
        + str(_STARTUP_SYS.version_info.minor)
        + ".zip"
        for prefix in (
            _STARTUP_SYS.base_prefix,
            _STARTUP_SYS.prefix,
            _STARTUP_SYS.exec_prefix,
        )
        if prefix
    )
    standard_library = []
    site_packages = []
    for raw_value in _STARTUP_SYS.path:
        if not isinstance(raw_value, str) or not raw_value.startswith("/"):
            continue
        value = raw_value.rstrip("/") or "/"
        if value == root or value.startswith(root + "/"):
            continue
        if value in zip_paths or any(
            value == library_root or value == library_root + "/lib-dynload"
            for library_root in library_roots
        ):
            if value not in standard_library:
                standard_library.append(value)
            continue
        if any(
            value == library_root + "/site-packages"
            or value.startswith(library_root + "/site-packages/")
            or value == library_root + "/dist-packages"
            or value.startswith(library_root + "/dist-packages/")
            for library_root in library_roots
        ) and value not in site_packages:
            site_packages.append(_validated_image_site_directory(value))
    user_site = _validated_image_site_directory(
        "/home/airflow/.local/lib/python" + version + "/site-packages"
    )
    if user_site not in site_packages:
        site_packages.append(user_site)
    application = [root, root + "/dags"] if include_application else []
    return standard_library + site_packages + application


if _STARTUP_SITE_PHASE != "direct":
    _STARTUP_SYS.path[:] = _trusted_python_paths(
        _STARTUP_ROOT,
        include_application=_STARTUP_SITE_PHASE == "finalize",
    )


_STARTUP_SYS.dont_write_bytecode = True
_STARTUP_SYS.pycache_prefix = "/__whoscored_runtime_bytecode_disabled__"


def _sha256_bytes(payload):
    constants = (
        0x428A2F98, 0x71374491, 0xB5C0FBCF, 0xE9B5DBA5,
        0x3956C25B, 0x59F111F1, 0x923F82A4, 0xAB1C5ED5,
        0xD807AA98, 0x12835B01, 0x243185BE, 0x550C7DC3,
        0x72BE5D74, 0x80DEB1FE, 0x9BDC06A7, 0xC19BF174,
        0xE49B69C1, 0xEFBE4786, 0x0FC19DC6, 0x240CA1CC,
        0x2DE92C6F, 0x4A7484AA, 0x5CB0A9DC, 0x76F988DA,
        0x983E5152, 0xA831C66D, 0xB00327C8, 0xBF597FC7,
        0xC6E00BF3, 0xD5A79147, 0x06CA6351, 0x14292967,
        0x27B70A85, 0x2E1B2138, 0x4D2C6DFC, 0x53380D13,
        0x650A7354, 0x766A0ABB, 0x81C2C92E, 0x92722C85,
        0xA2BFE8A1, 0xA81A664B, 0xC24B8B70, 0xC76C51A3,
        0xD192E819, 0xD6990624, 0xF40E3585, 0x106AA070,
        0x19A4C116, 0x1E376C08, 0x2748774C, 0x34B0BCB5,
        0x391C0CB3, 0x4ED8AA4A, 0x5B9CCA4F, 0x682E6FF3,
        0x748F82EE, 0x78A5636F, 0x84C87814, 0x8CC70208,
        0x90BEFFFA, 0xA4506CEB, 0xBEF9A3F7, 0xC67178F2,
    )
    state = [
        0x6A09E667,
        0xBB67AE85,
        0x3C6EF372,
        0xA54FF53A,
        0x510E527F,
        0x9B05688C,
        0x1F83D9AB,
        0x5BE0CD19,
    ]
    message = bytearray(payload)
    bit_length = len(message) * 8
    message.append(0x80)
    while len(message) % 64 != 56:
        message.append(0)
    message.extend(bit_length.to_bytes(8, "big"))

    def rotate(value, amount):
        return ((value >> amount) | (value << (32 - amount))) & 0xFFFFFFFF

    for offset in range(0, len(message), 64):
        words = [
            int.from_bytes(message[index : index + 4], "big")
            for index in range(offset, offset + 64, 4)
        ]
        for index in range(16, 64):
            previous_15 = words[index - 15]
            previous_2 = words[index - 2]
            sigma_0 = (
                rotate(previous_15, 7)
                ^ rotate(previous_15, 18)
                ^ (previous_15 >> 3)
            )
            sigma_1 = (
                rotate(previous_2, 17)
                ^ rotate(previous_2, 19)
                ^ (previous_2 >> 10)
            )
            words.append(
                (words[index - 16] + sigma_0 + words[index - 7] + sigma_1)
                & 0xFFFFFFFF
            )
        a, b, c, d, e, f, g, h = state
        for index, constant in enumerate(constants):
            choice = (e & f) ^ ((~e) & g)
            majority = (a & b) ^ (a & c) ^ (b & c)
            sum_0 = rotate(a, 2) ^ rotate(a, 13) ^ rotate(a, 22)
            sum_1 = rotate(e, 6) ^ rotate(e, 11) ^ rotate(e, 25)
            temporary_1 = (
                h + sum_1 + choice + constant + words[index]
            ) & 0xFFFFFFFF
            temporary_2 = (sum_0 + majority) & 0xFFFFFFFF
            h, g, f, e, d, c, b, a = (
                g,
                f,
                e,
                (d + temporary_1) & 0xFFFFFFFF,
                c,
                b,
                a,
                (temporary_1 + temporary_2) & 0xFFFFFFFF,
            )
        state = [
            (value + addition) & 0xFFFFFFFF
            for value, addition in zip(state, (a, b, c, d, e, f, g, h))
        ]
    return "".join(format(value, "08x") for value in state)


def _read_root_owned_trust_root(path):
    posix = _STARTUP_SYS.modules["posix"]
    if not path.startswith("/") or "//" in path:
        raise RuntimeError("WhoScored image trust-root path is not absolute")
    parts = tuple(part for part in path.split("/") if part)
    if not parts or any(part in (".", "..") for part in parts):
        raise RuntimeError("WhoScored image trust-root path is invalid")
    directory_flags = (
        posix.O_RDONLY
        | getattr(posix, "O_CLOEXEC", 0)
        | getattr(posix, "O_DIRECTORY", 0)
        | getattr(posix, "O_NOFOLLOW", 0)
    )
    file_flags = (
        posix.O_RDONLY
        | getattr(posix, "O_CLOEXEC", 0)
        | getattr(posix, "O_NOFOLLOW", 0)
    )
    descriptors = []
    try:
        descriptor = posix.open("/", directory_flags)
        descriptors.append(descriptor)
        for part in parts[:-1]:
            descriptor = posix.open(part, directory_flags, dir_fd=descriptor)
            descriptors.append(descriptor)
            metadata = posix.fstat(descriptor)
            if metadata.st_mode & 0o170000 != 0o040000:
                raise RuntimeError("WhoScored image trust-root parent is not a directory")
            if _STARTUP_ENFORCE_TRUST_OWNERSHIP and (
                metadata.st_uid != 0 or metadata.st_mode & 0o022
            ):
                raise RuntimeError("WhoScored image trust-root parent is mutable")
        file_descriptor = posix.open(parts[-1], file_flags, dir_fd=descriptor)
        descriptors.append(file_descriptor)
        before = posix.fstat(file_descriptor)
        if before.st_mode & 0o170000 != 0o100000:
            raise RuntimeError("WhoScored image trust root is not a regular file")
        if _STARTUP_ENFORCE_TRUST_OWNERSHIP and (
            before.st_uid != 0 or before.st_mode & 0o777 != 0o444
        ):
            raise RuntimeError("WhoScored image trust root is not root-owned 0444")
        if before.st_size <= 0 or before.st_size > 4096:
            raise RuntimeError("WhoScored image trust-root size is invalid")
        chunks = []
        while True:
            chunk = posix.read(file_descriptor, 4096)
            if not chunk:
                break
            chunks.append(chunk)
        after = posix.fstat(file_descriptor)
    finally:
        for open_descriptor in reversed(descriptors):
            posix.close(open_descriptor)
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
        raise RuntimeError("WhoScored image trust root changed during fd read")
    return b"".join(chunks)


def _read_mounted_runtime_file(root, relative):
    posix = _STARTUP_SYS.modules["posix"]
    parts = tuple(relative.split("/"))
    if not parts or any(not part or part in (".", "..") for part in parts):
        raise RuntimeError("WhoScored mounted runtime path is invalid")
    directory_flags = (
        posix.O_RDONLY
        | getattr(posix, "O_CLOEXEC", 0)
        | getattr(posix, "O_DIRECTORY", 0)
        | getattr(posix, "O_NOFOLLOW", 0)
    )
    file_flags = (
        posix.O_RDONLY
        | getattr(posix, "O_CLOEXEC", 0)
        | getattr(posix, "O_NOFOLLOW", 0)
    )
    descriptors = []
    try:
        descriptor = posix.open(root, directory_flags)
        descriptors.append(descriptor)
        for part in parts[:-1]:
            descriptor = posix.open(part, directory_flags, dir_fd=descriptor)
            descriptors.append(descriptor)
        file_descriptor = posix.open(parts[-1], file_flags, dir_fd=descriptor)
        descriptors.append(file_descriptor)
        before = posix.fstat(file_descriptor)
        if before.st_mode & 0o170000 != 0o100000:
            raise RuntimeError("WhoScored mounted runtime member is not regular")
        chunks = []
        while True:
            chunk = posix.read(file_descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = posix.fstat(file_descriptor)
    finally:
        for open_descriptor in reversed(descriptors):
            posix.close(open_descriptor)
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if any(getattr(before, field) != getattr(after, field) for field in stable_fields):
        raise RuntimeError("WhoScored mounted runtime member changed during fd read")
    return b"".join(chunks)


def _trusted_release_material(root):
    raw_trust_root = _read_root_owned_trust_root(_STARTUP_TRUST_ROOT)
    try:
        text = raw_trust_root.decode("ascii")
    except UnicodeDecodeError as exc:
        raise RuntimeError("WhoScored image trust root is not ASCII") from exc
    lines = text.splitlines()
    expected_keys = (
        "schema_version",
        "runtime_class",
        "runtime_contract_source_sha256",
        "runtime_contract_lock_sha256",
    )
    if text != "\n".join(lines) + "\n" or len(lines) != len(expected_keys):
        raise RuntimeError("WhoScored image trust-root format is not canonical")
    values = {}
    for line, expected_key in zip(lines, expected_keys):
        key, separator, value = line.partition("=")
        if separator != "=" or key != expected_key or not value:
            raise RuntimeError("WhoScored image trust-root schema is invalid")
        values[key] = value
    hex_characters = frozenset("0123456789abcdef")
    if (
        values["schema_version"] != "1"
        or values["runtime_class"]
        not in ("generic-v1", "test-v1", "production-v1")
        or any(
            len(values[key]) != 64
            or any(character not in hex_characters for character in values[key])
            for key in (
                "runtime_contract_source_sha256",
                "runtime_contract_lock_sha256",
            )
        )
    ):
        raise RuntimeError("WhoScored image trust-root identity is invalid")
    source = _read_mounted_runtime_file(
        root, "scrapers/whoscored/runtime_contract.py"
    )
    lock = _read_mounted_runtime_file(
        root, "scrapers/whoscored/runtime_contract.lock"
    )
    if _sha256_bytes(source) != values["runtime_contract_source_sha256"]:
        raise RuntimeError(
            "WhoScored mounted runtime attestor differs from the image trust root"
        )
    if _sha256_bytes(lock) != values["runtime_contract_lock_sha256"]:
        raise RuntimeError(
            "WhoScored mounted runtime lock differs from the image trust root"
        )
    return values, source


def _run_production_gate(runtime_class):
    if runtime_class != "production-v1":
        return
    posix = _STARTUP_SYS.modules["posix"]
    try:
        child = posix.fork()
    except OSError as exc:
        raise RuntimeError("cannot fork WhoScored production gate") from exc
    if child == 0:
        try:
            posix.execve(
                _STARTUP_PRODUCTION_GATE,
                (_STARTUP_PRODUCTION_GATE,),
                {},
            )
        except BaseException:
            posix._exit(78)
    try:
        waited, status = posix.waitpid(child, 0)
    except OSError as exc:
        raise RuntimeError("cannot wait for WhoScored production gate") from exc
    if waited != child or status != 0:
        raise RuntimeError(
            "WhoScored production gate rejected this image: " + str(status)
        )


def _load_whoscored_runtime_contract(root=None):
    expected_root = _STARTUP_ROOT
    requested_root = str(root or expected_root).rstrip("/")
    if requested_root != expected_root:
        raise RuntimeError(
            "WhoScored startup anchor is bound to one runtime root: "
            + requested_root
        )
    _STARTUP_SYS.path[:] = _trusted_python_paths(requested_root)
    cached = _STARTUP_PRIVATE["canonical"]
    private_attestor = _STARTUP_PRIVATE["attestor"]
    if cached is not None:
        if getattr(
            _STARTUP_SYS, "_load_whoscored_runtime_contract", None
        ) is not _load_whoscored_runtime_contract:
            raise RuntimeError("WhoScored image runtime loader was replaced")
        if getattr(
            _STARTUP_SYS, "_require_whoscored_runtime_class", None
        ) is not _require_whoscored_runtime_class:
            raise RuntimeError("WhoScored image runtime-class verifier was replaced")
        if getattr(_STARTUP_SYS, "_whoscored_runtime_contract", None) is not cached:
            raise RuntimeError("WhoScored canonical runtime cache was replaced")
        if _STARTUP_REQUIRE_FULL and getattr(
            _STARTUP_SYS, "_whoscored_runtime_class", None
        ) != _STARTUP_PRIVATE["runtime_class"]:
            raise RuntimeError("WhoScored runtime-class marker was replaced")
        if _STARTUP_REQUIRE_FULL:
            if private_attestor is None:
                raise RuntimeError("WhoScored private image attestor is unavailable")
            private_attestor.validate_runtime_contract(report_schema_version=3)
            private_attestor.validate_runtime_import_boundary(
                runtime_root=requested_root
            )
        else:
            private_attestor.validate_runtime_import_boundary(
                runtime_root=requested_root
            )
        return cached

    bootstrap = _STARTUP_SYS.modules["_frozen_importlib"]
    external = _STARTUP_SYS.modules["_frozen_importlib_external"]
    if _STARTUP_REQUIRE_FULL:
        trust_root, source = _trusted_release_material(requested_root)
        _run_production_gate(trust_root["runtime_class"])
    else:
        trust_root = None
        source = _read_mounted_runtime_file(
            requested_root, "scrapers/whoscored/runtime_contract.py"
        )

    path = requested_root + "/scrapers/whoscored/runtime_contract.py"
    loader = external.SourceFileLoader(
        "_whoscored_runtime_contract_attestor",
        path,
    )
    spec = bootstrap.ModuleSpec(loader.name, loader, origin=path)
    spec.has_location = True
    attestor = bootstrap.module_from_spec(spec)
    if trust_root is not None:
        attestor.__dict__["_IMAGE_TRUSTED_RUNTIME_CONTRACT_SHA256"] = trust_root[
            "runtime_contract_lock_sha256"
        ]
    exec(compile(source, path, "exec", dont_inherit=True), attestor.__dict__)
    if _STARTUP_REQUIRE_FULL:
        attestor.validate_runtime_contract(report_schema_version=3)
    else:
        attestor.validate_runtime_import_boundary(runtime_root=requested_root)
    canonical = __import__(
        "scrapers.whoscored.runtime_contract",
        fromlist=("runtime_contract",),
    )
    attestor.validate_runtime_import_boundary(runtime_root=requested_root)
    _STARTUP_PRIVATE["attestor"] = attestor
    _STARTUP_PRIVATE["canonical"] = canonical
    if trust_root is not None:
        runtime_class = trust_root["runtime_class"]
        existing_runtime_class = getattr(
            _STARTUP_SYS, "_whoscored_runtime_class", None
        )
        if existing_runtime_class not in (None, runtime_class):
            raise RuntimeError("WhoScored runtime-class marker was replaced")
        _STARTUP_SYS._whoscored_runtime_class = runtime_class
        _STARTUP_PRIVATE["runtime_class"] = runtime_class
    _STARTUP_SYS._whoscored_runtime_contract = canonical
    return canonical


def _require_whoscored_runtime_class(required_class, operation):
    """Check the image-private class, not only the mutable public marker."""

    if required_class not in ("generic-v1", "test-v1", "production-v1"):
        raise RuntimeError("invalid required WhoScored runtime class")
    if not isinstance(operation, str) or not operation.strip():
        raise RuntimeError("WhoScored runtime-class operation is invalid")
    if not _STARTUP_REQUIRE_FULL:
        raise RuntimeError(
            "WhoScored runtime class is unavailable without full image attestation"
        )
    if getattr(
        _STARTUP_SYS, "_load_whoscored_runtime_contract", None
    ) is not _load_whoscored_runtime_contract:
        raise RuntimeError("WhoScored image runtime loader was replaced")
    if getattr(
        _STARTUP_SYS, "_require_whoscored_runtime_class", None
    ) is not _require_whoscored_runtime_class:
        raise RuntimeError("WhoScored image runtime-class verifier was replaced")
    if _STARTUP_PRIVATE["runtime_class"] is None:
        _load_whoscored_runtime_contract(_STARTUP_ROOT)
    actual_class = _STARTUP_PRIVATE["runtime_class"]
    if getattr(_STARTUP_SYS, "_whoscored_runtime_class", None) != actual_class:
        raise RuntimeError("WhoScored runtime-class marker was replaced")
    if actual_class != required_class:
        raise RuntimeError(
            operation
            + " requires WhoScored runtime class "
            + required_class
            + "; actual="
            + str(actual_class)
        )
    if required_class == "production-v1":
        _run_production_gate(actual_class)
    return actual_class


_STARTUP_SYS._whoscored_runtime_startup_schema = 2
_STARTUP_SYS._whoscored_runtime_startup_root = _STARTUP_ROOT
_STARTUP_SYS._load_whoscored_runtime_contract = _load_whoscored_runtime_contract
_STARTUP_SYS._require_whoscored_runtime_class = _require_whoscored_runtime_class

# Every image-started interpreter proves the root-owned runtime class and runs
# the production gate before user code, even when argv0 is not ``airflow``.
if _STARTUP_SITE_PHASE != "direct" and _STARTUP_REQUIRE_FULL:
    _site_trust_root, _site_source = _trusted_release_material(_STARTUP_ROOT)
    _run_production_gate(_site_trust_root["runtime_class"])

# Airflow imports DAGs and plugins after Python site initialization.  Hash the
# complete WhoScored closure here for Airflow console processes, before any of
# that mutable application code is importable. Standalone WhoScored scripts
# invoke the same anchor as their first executable statement.
_startup_argv0 = str(_STARTUP_SYS.argv[0] if _STARTUP_SYS.argv else "")
if _STARTUP_REQUIRE_FULL and (
    _startup_argv0 == "airflow" or _startup_argv0.endswith("/airflow")
):
    _load_whoscored_runtime_contract(_STARTUP_ROOT)
if _STARTUP_SITE_PHASE == "bootstrap":
    _STARTUP_SYS.path[:] = _trusted_python_paths(
        _STARTUP_ROOT,
        include_application=False,
    )
