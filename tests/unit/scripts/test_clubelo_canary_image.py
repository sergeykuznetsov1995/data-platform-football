from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CANARY_DIR = ROOT / "docker" / "images" / "flaresolverr-canary"
DOCKERFILE = CANARY_DIR / "Dockerfile"
ENTRYPOINT = CANARY_DIR / "entrypoint.sh"
WHOSCORED_ENTRYPOINT = (
    ROOT / "docker" / "images" / "flaresolverr-whoscored" / "entrypoint.sh"
)


def dockerfile_text():
    return DOCKERFILE.read_text(encoding="utf-8")


def dockerfile_instructions():
    return [
        line
        for line in dockerfile_text().splitlines()
        if line and not line[0].isspace()
    ]


def entrypoint_text():
    return ENTRYPOINT.read_text(encoding="utf-8")


def whoscored_safety_block():
    lines = WHOSCORED_ENTRYPOINT.read_text(encoding="utf-8").splitlines()
    start = next(
        index for index, line in enumerate(lines)
        if line.startswith("extension=")
    )
    end = next(
        index for index, line in enumerate(lines)
        if line.startswith("install -m 0700")
    )
    return [line for line in lines[start : end + 2] if line.strip()]


def test_manifest_files_exist():
    assert DOCKERFILE.is_file()
    assert ENTRYPOINT.is_file()
    assert WHOSCORED_ENTRYPOINT.is_file()


def test_dockerfile_uses_base_image_arg():
    instructions = dockerfile_instructions()
    assert instructions[0] == "ARG BASE_IMAGE"
    assert instructions[1] == "FROM ${BASE_IMAGE}"


def test_dockerfile_switches_to_root_before_copy_and_run():
    instructions = dockerfile_instructions()
    root_at = instructions.index("USER root")
    first_copy = min(
        index
        for index, line in enumerate(instructions)
        if line.startswith("COPY ")
    )
    first_run = min(
        index
        for index, line in enumerate(instructions)
        if line.startswith("RUN ")
    )
    assert root_at < first_copy < first_run


def test_dockerfile_copies_exact_sources_and_destinations():
    instructions = dockerfile_instructions()
    copy_lines = [
        line for line in instructions if line.startswith("COPY ")
    ]
    assert len(copy_lines) == 2
    assert (
        "COPY scripts/research/clubelo_canary_solver_instrumentation.py "
        "/usr/local/libexec/clubelo-canary/solver_instrumentation.py" in copy_lines
    )
    assert (
        "COPY docker/images/flaresolverr-canary/entrypoint.sh "
        "/usr/local/bin/clubelo-canary-entrypoint" in copy_lines
    )


def test_dockerfile_has_single_network_none_run():
    run_lines = [
        line for line in dockerfile_instructions()
        if line.startswith("RUN ")
    ]
    assert len(run_lines) == 1
    assert run_lines[0].startswith("RUN --network=none ")


def test_dockerfile_run_fixes_ownership_and_modes():
    text = dockerfile_text()
    assert (
        "chown 0:0 /usr/local/libexec/clubelo-canary/solver_instrumentation.py "
        "/usr/local/bin/clubelo-canary-entrypoint" in text
    )
    assert (
        "chmod 0444 /usr/local/libexec/clubelo-canary/solver_instrumentation.py"
        in text
    )
    assert "chmod 0555 /usr/local/bin/clubelo-canary-entrypoint" in text
    assert (
        "install -d -o 1000 -g 1000 -m 0700 /var/lib/clubelo-canary-run"
        in text
    )


def test_dockerfile_no_package_installs_or_downloads():
    lowered = dockerfile_text().lower()
    for banned in (
        "apt",
        "pip",
        "apk",
        "dnf",
        "yum",
        "curl",
        "wget",
        "git ",
        "add ",
        "http://",
        "https://",
    ):
        assert banned not in lowered


def test_dockerfile_preserves_driver_before_replacing_with_tmpfs_symlink():
    # Pin the complete shell chain: any failed copy/removal/link must fail
    # the build, and the existing control symlink must take a no-op branch.
    text = dockerfile_text().replace("\\\n", " ")
    run_lines = [line for line in text.splitlines() if line.startswith("RUN ")]
    assert len(run_lines) == 1
    assert " ".join(run_lines[0].split()) == (
        "RUN --network=none "
        "chown 0:0 /usr/local/libexec/clubelo-canary/solver_instrumentation.py "
        "/usr/local/bin/clubelo-canary-entrypoint "
        "&& chmod 0444 /usr/local/libexec/clubelo-canary/solver_instrumentation.py "
        "&& chmod 0555 /usr/local/bin/clubelo-canary-entrypoint "
        "&& install -d -o 1000 -g 1000 -m 0700 /var/lib/clubelo-canary-run "
        "&& if test -f /app/chromedriver && test ! -L /app/chromedriver; then "
        "install -d -o 0 -g 0 -m 0755 /usr/local/share/clubelo-canary "
        "&& install -o 0 -g 0 -m 0444 /app/chromedriver "
        "/usr/local/share/clubelo-canary/chromedriver.original "
        "&& rm /app/chromedriver "
        "&& ln -s /tmp/clubelo-canary-chromedriver /app/chromedriver "
        "|| exit 1; "
        "elif test -L /app/chromedriver "
        '&& test "$(readlink /app/chromedriver)" = /tmp/whoscored-chromedriver; then '
        ":; else exit 1; fi"
    )


def test_dockerfile_dumb_init_entrypoint_and_fixed_cmd():
    instructions = dockerfile_instructions()
    assert 'ENTRYPOINT ["/usr/bin/dumb-init", "--"]' in instructions
    assert 'CMD ["/usr/local/bin/clubelo-canary-entrypoint"]' in instructions


def test_dockerfile_final_user_non_root():
    instructions = dockerfile_instructions()
    user_indexes = [
        index
        for index, line in enumerate(instructions)
        if line.startswith("USER ")
    ]
    assert user_indexes
    assert instructions[user_indexes[-1]] == "USER 1000:1000"


def test_entrypoint_strict_shell():
    lines = entrypoint_text().splitlines()
    assert lines[0] == "#!/bin/sh"
    assert "set -eu" in lines
    assert "set +e" not in lines


def test_entrypoint_preserves_whoscored_safety_block_exactly():
    canary_lines = entrypoint_text().splitlines()
    non_blank = [line for line in canary_lines if line.strip()]
    positions = []
    for line in whoscored_safety_block():
        assert line in canary_lines
        positions.append(non_blank.index(line))
    assert positions == sorted(positions)
    assert all(
        later - earlier == 1
        for earlier, later in zip(positions, positions[1:])
    )


def test_entrypoint_safety_block_gated_on_control_only():
    lines = entrypoint_text().splitlines()
    control_gate = next(
        index
        for index, line in enumerate(lines)
        if "CLUBELO_CANARY_UPSTREAM" in line and "control" in line
    )
    candidate_gate = next(
        index
        for index, line in enumerate(lines)
        if "CLUBELO_CANARY_UPSTREAM" in line and "candidate" in line
    )
    safety_start = lines.index(
        "extension=/usr/local/libexec/whoscored/flaresolverr_extended.py"
    )
    install_line = next(
        index
        for index, line in enumerate(lines)
        if line.startswith("install -m 0700")
    )
    assert control_gate < safety_start < install_line < candidate_gate
    candidate_end = lines.index("else", candidate_gate)
    candidate_block = "\n".join(lines[candidate_gate:candidate_end])
    assert "upstream=/app/flaresolverr.py" in candidate_block
    for whoscored_only in (
        "sha256sum",
        "whoscored",
        "flaresolverr-extension",
    ):
        assert whoscored_only not in candidate_block


def test_entrypoint_candidate_validates_original_then_installs_tmpfs_driver():
    lines = entrypoint_text().splitlines()
    candidate_gate = lines.index(
        'elif test "${CLUBELO_CANARY_UPSTREAM:-}" = candidate; then'
    )
    candidate_end = lines.index("else", candidate_gate)
    # Exact ordered branch pins checks before the only candidate copy and
    # prevents an intervening command from disabling fail-closed startup.
    assert lines[candidate_gate + 1:candidate_end] == [
        "driver_original=/usr/local/share/clubelo-canary/chromedriver.original",
        "test -L /app/chromedriver",
        'test "$(readlink /app/chromedriver)" = /tmp/clubelo-canary-chromedriver',
        'test -f "$driver_original"',
        'test ! -L "$driver_original"',
        'test "$(stat -c \'%u:%g:%a:%h\' "$driver_original")" = \'0:0:444:1\'',
        'install -m 0700 "$driver_original" /tmp/clubelo-canary-chromedriver',
        "upstream=/app/flaresolverr.py",
    ]
    assert candidate_end < next(
        index for index, line in enumerate(lines)
        if line.startswith("exec /usr/local/bin/python")
    )
    assert sum(line.startswith("install -m 0700") for line in lines) == 2


def test_entrypoint_candidate_uses_fixed_candidate_upstream():
    text = entrypoint_text()
    assert '"candidate": "/app/flaresolverr.py"' in text
    assert text.count("/app/flaresolverr.py") == 2
    assert "upstream=/app/flaresolverr.py" in text.splitlines()


def test_entrypoint_rejects_unknown_upstream_nonzero():
    text = entrypoint_text()
    lines = text.splitlines()
    assert (
        'echo "clubelo-canary: CLUBELO_CANARY_UPSTREAM must be control '
        'or candidate" >&2' in lines
    )
    assert "exit 1" in lines
    assert "mode not in UPSTREAMS" in text
    assert "raise SystemExit(2)" in text
    assert lines.index("exit 1") < next(
        index
        for index, line in enumerate(lines)
        if line.startswith("exec /usr/local/bin/python")
    )


def test_entrypoint_uses_isolated_python_bootstrap():
    text = entrypoint_text()
    assert text.count("exec /usr/local/bin/python") == 1
    assert "exec /usr/local/bin/python -I -u -c" in text


def test_entrypoint_bootstrap_maps_fixed_upstreams_only():
    text = entrypoint_text()
    assert (
        '"control": "/usr/local/libexec/whoscored/flaresolverr_extended.py"'
        in text
    )
    assert 'os.environ.get("CLUBELO_CANARY_UPSTREAM", "")' in text
    assert text.count("os.environ") == 1


def test_image_has_no_browser_budget_or_cdp_integration():
    for text in (dockerfile_text(), entrypoint_text()):
        for forbidden in (
            "browserbudgetgate", "browser_budget", "clubelo-canary-budget",
            "budget.json", "counter.json", "budget_path", "counter_path",
            "clubelo_canary_revision", "armed_revision", "cdp", "devtools",
            "fetch.enable", "network.enable", "admitted_requests",
            "blocked_requests",
        ):
            assert forbidden not in text.lower()


def test_entrypoint_installs_observer_then_runs_upstream():
    text = entrypoint_text()
    assert "import utils" not in text
    assert text.count("solver_instrumentation.install(") == 1
    assert text.count("runpy.run_path") == 1
    assert 'runpy.run_path(upstream, run_name="__main__")' in text
    app_path_at = text.index('sys.path.insert(0, "/app")')
    active_run_at = text.index("active_run.json")
    observer_install_at = text.index("solver_instrumentation.install(")
    run_at = text.index("runpy.run_path")
    assert app_path_at < active_run_at < observer_install_at < run_at


def test_entrypoint_checks_upstream_file_exists_non_symlink():
    text = entrypoint_text()
    assert 'test -f "$upstream" && test ! -L "$upstream"' in text


def test_entrypoint_no_dynamic_code_or_network():
    text = entrypoint_text()
    for banned in (
        "eval(",
        "exec(",
        "urllib",
        "socket",
        "requests",
        "subprocess",
        "os.system",
        "popen",
    ):
        assert banned not in text


def test_entrypoint_no_secret_logging():
    text = entrypoint_text()
    assert "print(" not in text
    for message in (
        "clubelo-canary: invalid CLUBELO_CANARY_UPSTREAM",
        "clubelo-canary: missing or invalid active_run.json",
    ):
        assert message in text


def test_dockerfile_creates_only_observer_and_immutable_driver_dirs():
    text = dockerfile_text()
    assert (
        "install -d -o 1000 -g 1000 -m 0700 /var/lib/clubelo-canary-run"
        in text
    )
    assert (
        "install -d -o 0 -g 0 -m 0755 /usr/local/share/clubelo-canary"
        in text
    )
    assert text.count("install -d") == 2


def test_entrypoint_observer_uses_fixed_dir():
    text = entrypoint_text()
    assert text.count('state_dir="/var/lib/clubelo-canary-run"') == 1
    observer_block = text[
        text.index("solver_instrumentation.install(") : text.index("runpy.run_path")
    ]
    assert 'state_dir="/var/lib/clubelo-canary-run"' in observer_block
    assert text.count("state_dir=") == 1


def test_entrypoint_imports_solver_observer_by_fixed_path_only():
    text = entrypoint_text()
    assert text.count("spec_from_file_location") == 1
    assert text.count('"clubelo_canary_solver_instrumentation"') == 1
    assert (
        text.count(
            '"/usr/local/libexec/clubelo-canary/solver_instrumentation.py"'
        ) == 1
    )
    assert "module_from_spec" in text
    assert "exec_module" in text


def test_entrypoint_solver_observer_install_uses_fixed_arm_mode():
    text = entrypoint_text()
    assert 'state_dir="/var/lib/clubelo-canary-run", arm=mode' in text
    assert text.count("arm=mode") == 1
    assert 'mode = os.environ.get("CLUBELO_CANARY_UPSTREAM", "")' in text
    assert "if mode not in UPSTREAMS" in text


def test_entrypoint_requires_regular_active_run_before_upstream():
    text = entrypoint_text()
    # One occurrence is the validated path; the other is the fail-closed message.
    assert text.count("active_run.json") == 2
    assert '"/var/lib/clubelo-canary-run/active_run.json"' in text
    assert "follow_symlinks=False" in text
    assert "stat.S_ISREG(active_info.st_mode)" in text
    assert "except OSError:" in text
    assert "if active_info is None or not stat.S_ISREG(active_info.st_mode):" in text
    assert text.count("raise SystemExit(2)") == 2
    assert text.index("active_run.json") < text.index("spec_from_file_location")


def test_entrypoint_does_not_create_or_clear_active_run():
    text = entrypoint_text()
    for banned in (
        "os.remove",
        "unlink",
        "rmtree",
        "truncate",
        "O_WRONLY",
        "O_CREAT",
        "open(",
    ):
        assert banned not in text


def test_entrypoint_registers_observer_module_before_execution():
    text = entrypoint_text()
    assert "sys.modules[solver_spec.name] = solver_instrumentation" in text
    assert text.count("sys.modules[") == 1
    assert text.index("sys.modules[solver_spec.name] = solver_instrumentation") < text.index(
        "solver_spec.loader.exec_module(solver_instrumentation)"
    )
    assert text.index("solver_spec.loader.exec_module(solver_instrumentation)") < text.index(
        "solver_instrumentation.install("
    )
