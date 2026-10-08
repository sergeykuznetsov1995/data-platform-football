"""Future host integration; importing this module performs no I/O."""

import os
from pathlib import Path
import subprocess


def fbref_daily_report() -> list[str]:
    root = Path(os.environ.get("FBREF_DAILY_REPORT_ROOT", "/root/data-platform-football/.local/releases/fbref-1323"))
    output = Path("/root/data-platform-football/.local/reports/fbref/daily")
    script = root / "scripts/report_fbref_daily.py"
    if not script.is_file():
        return ["• FBref #1323: измеритель ещё не доставлен; приёмка не подтверждена ⚠️"]
    try:
        result = subprocess.run([
            os.environ.get("FBREF_REPORT_PYTHON", "/usr/bin/python3"), "-B", str(script),
            "--host-read-only", "--output-dir", str(output),
        ], capture_output=True, text=True, timeout=180, check=True)
        lines = result.stdout.strip().splitlines()
        if len(lines) != 3 or not lines[0].startswith("FBref: матчей "):
            raise ValueError("Unexpected report output")
        return ["• " + line for line in lines]
    except (OSError, subprocess.SubprocessError, ValueError):
        return ["• FBref #1323: отчёт недоступен; приёмка не подтверждена ⚠️"]
