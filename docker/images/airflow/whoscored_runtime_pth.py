"""Fatal wrapper for the image-owned WhoScored ``.pth`` hooks."""

import sys


STARTUP_PATH = "/usr/local/lib/whoscored_runtime_startup.py"
EXIT_CONFIG = 78


def run(
    phase: str,
    *,
    startup_path: str = STARTUP_PATH,
    runtime_root=None,
    require_full=None,
    trust_root_path=None,
    enforce_trust_ownership=None,
    production_gate_path=None,
) -> None:
    """Execute the anchor and terminate the interpreter on every failure."""

    namespace = {
        "__builtins__": __builtins__,
        "sys": sys,
        "_WHOSCORED_SITE_PHASE": phase,
    }
    optional = {
        "_WHOSCORED_RUNTIME_ROOT": runtime_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": require_full,
        "_WHOSCORED_TRUST_ROOT_PATH": trust_root_path,
        "_WHOSCORED_ENFORCE_TRUST_OWNERSHIP": enforce_trust_ownership,
        "_WHOSCORED_PRODUCTION_GATE_PATH": production_gate_path,
    }
    namespace.update({key: value for key, value in optional.items() if value is not None})
    try:
        with open(startup_path, "rb") as handle:
            source = handle.read()
        exec(compile(source, startup_path, "exec"), namespace)
    except BaseException:
        # site.addpackage catches Exception and otherwise continues startup.
        # _exit is deliberate: no trailing .pth or application byte may run.
        sys.modules["posix"]._exit(EXIT_CONFIG)
